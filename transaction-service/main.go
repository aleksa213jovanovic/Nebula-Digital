package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

func main() {
	fmt.Printf("RUNNING TRANSACTION SERVICE\n")

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic %v. stacktrace: %s\n", r, string(debug.Stack()))
			log.Fatal()
		}
	}()

	dbUrl, err := buildDBUrl()
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	fmt.Printf("establishing db connection %s\n", dbUrl)

	db, err := sql.Open("postgres", dbUrl)

	if err != nil {
		log.Fatal("error connecting to database: ", err)
	}

	err = db.Ping()

	if err != nil {
		log.Fatal("error pinging the database: ", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	defer db.Close()

	fmt.Printf("db connection succesfully established\n")

	fmt.Printf("connecting to nats\n")

	natsUri, err := GetErr("NATS_URI")
	if err != nil {
		log.Fatal(err)
	}

	// natsUri := "http://localhost:4222"
	nc, err := nats.Connect(natsUri)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	fmt.Printf("succesfully connected to nats\n")

	kafkaCtx, cancelKafka := context.WithCancel(context.Background())

	go setKafkaConsumer(db, kafkaCtx)
	setNatsWorker(db, nc)

	server, err := setRouter(db, nc)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	//  Block until we receive our signal
	<-stopChan
	cancelKafka()
	fmt.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	fmt.Println("Server gracefully stopped")

}

func setNatsWorker(db *sql.DB, nc *nats.Conn) {

	_, err := nc.Subscribe("userBalance", func(m *nats.Msg) {
		log.Printf("Received a message: %s\n", string(m.Data))

		type request struct {
			UserId int `json:"userId"`
		}

		req := request{}

		err := json.Unmarshal(m.Data, &req)
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}

		query := fmt.Sprintf("SELECT balance FROM users WHERE user_id='%d'", req.UserId)

		var userBalance float64
		err = db.QueryRow(query).Scan(&userBalance)
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}

		type response struct {
			UserBalance float64 `json:"userBalance"`
		}

		b := response{
			UserBalance: userBalance,
		}

		payload, err := json.Marshal(b)
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}

		err = nc.Publish(m.Reply, payload)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}

func setRouter(db *sql.DB, nc *nats.Conn) (*http.Server, error) {

	// port := "8081"
	port, err := GetErr("APP_PORT")
	if err != nil {
		return nil, err
	}

	server := http.Server{
		Addr: fmt.Sprintf(":%s", port),
	}

	h := handler{
		db:   db,
		nats: nc,
	}

	http.HandleFunc("/transaction-service/fetch/user", h.fetchUser)
	http.HandleFunc("/transaction-service/user/credit", h.userCredit)
	http.HandleFunc("/transaction-service/user/transfer", h.userTransfer)

	go func() {
		log.Printf("Starting server on :%s\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("%v\n", err)
			return
		}
	}()

	return &server, nil
}

type handler struct {
	db   *sql.DB
	nats *nats.Conn
}

type User struct {
	Email  string `json:"email"`
	UserId int    `json:"user_id"`
}

func (h *handler) userTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	type UserTransfer struct {
		FromId int     `json:"from_user_id"`
		ToId   int     `json:"to_user_id"`
		Amount float64 `json:"amount_to_transfer"`
	}

	var req UserTransfer
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = transfer(h.db, req.FromId, req.ToId, req.Amount)
	if err != nil {
		fmt.Printf("%v\n", err)
		http.Error(w, "fail", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func transfer(db *sql.DB, fromId int, toId int, amount float64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec("UPDATE users SET balance = balance + $1 WHERE user_id = $2", -amount, fromId)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, toId)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("INSERT INTO transactions (user_id, amount, transaction_type) VALUES ($1, $2, 'debit')", fromId, amount)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("INSERT INTO transactions (user_id, amount, transaction_type) VALUES ($1, $2, 'credit')", toId, amount)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (h *handler) userCredit(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	type UserCredit struct {
		UserId int     `json:"user_id"`
		Amount float64 `json:"amount"`
	}

	var req UserCredit
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newBalance, err := credit(h.db, req.UserId, req.Amount)
	if err != nil {
		http.Error(w, "fail", http.StatusInternalServerError)
		return
	}

	type response struct {
		UpdatedBalance float64 `json:"updated_balance:"`
	}

	jsonResponse, err := json.Marshal(response{UpdatedBalance: newBalance})
	if err != nil {
		fmt.Printf("%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)

}

func credit(db *sql.DB, userId int, amount float64) (float64, error) {
	var newBalance float64

	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}

	err = tx.QueryRow("UPDATE users SET balance = balance + $1 WHERE user_id = $2 RETURNING balance", amount, userId).Scan(&newBalance)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	_, err = tx.Exec("INSERT INTO transactions (user_id, amount, transaction_type) VALUES ($1, $2, 'credit')", userId, amount)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	// Commit the transaction
	return newBalance, tx.Commit()
}

func (h *handler) fetchUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	userId, err := strconv.Atoi(query.Get("userId"))
	if err != nil {
		http.Error(w, "missing userId in query", http.StatusBadRequest)
		return
	}

	user, err := FindUser(h.db, userId)
	if err != nil {
		http.Error(w, "fail", http.StatusInternalServerError)
		return
	}

	jsonResponse, err := json.Marshal(user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)

}

type FoundUser struct {
	Balance string `json:"balance"`
	UserId  int    `json:"user_id"`
}

func FindUser(db *sql.DB, userId int) (*FoundUser, error) {
	var user FoundUser

	// SQL query to find the user
	query := `SELECT user_id, balance, created_at FROM users WHERE user_id = $1`

	// Execute the query
	row := db.QueryRow(query, userId)
	err := row.Scan(&user.UserId, &user.Balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no user found with userid %s", userId)
		}
		return nil, err
	}

	return &user, nil
}

func setKafkaConsumer(db *sql.DB, ctx context.Context) {
	conf := ReadConfig("./kafka.server.properties")
	conf["group.id"] = "kafka-go-getting-started"
	conf["auto.offset.reset"] = "earliest"

	c, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}
	defer c.Close()

	userCreateTopic := "userCreate"
	err = c.SubscribeTopics([]string{userCreateTopic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	run := true
	for run {
		select {
		case <-ctx.Done():
			log.Println("kafka consumer stopped")
			return
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.Error() != "Local: Timed out" {
					fmt.Printf("%v\n", err)
				}
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

			if *ev.TopicPartition.Topic == userCreateTopic {

				newUser := &User{}
				err = json.Unmarshal(ev.Value, newUser)
				if err != nil {
					fmt.Printf("Error unmarshalling JSON: %v", err)
					continue
				}

				// todo check for error and notify user service

				_, err = InsertUser(db, newUser)
				if err != nil {
					fmt.Printf("%v\n", err)
				}
			}
		}
	}
}

func InsertUser(db *sql.DB, user *User) (int, error) {
	// SQL statement to insert a new user
	query := `INSERT INTO users (user_id) VALUES ($1) RETURNING user_id`

	var userID int
	err := db.QueryRow(query, user.UserId).Scan(&userID)
	if err != nil {
		// Check if the error is due to a duplicate entry
		if pqErr, ok := err.(*pq.Error); ok {
			// Here "23505" is the PostgreSQL error code for "unique_violation"
			if pqErr.Code == "23505" {
				return -1, fmt.Errorf("user already exists")
			}
		}
		return -1, err
	}

	return userID, nil
}

func buildDBUrl() (string, error) {
	// return "postgres://user:password@localhost:5438/transactionservice?sslmode=disable", nil

	dbUrl := ""
	host, err := GetErr("DB_HOST")
	if err != nil {
		return "", err
	}
	user, err := GetErr("DB_USER")
	if err != nil {
		return "", err
	}
	dbname, err := GetErr("DB_NAME")
	if err != nil {
		return "", err
	}
	password, err := GetErr("DB_PASSWORD")
	if err != nil {
		return "", err
	}

	port, err := GetErr("DB_PORT")
	if err != nil {
		return "", err
	}

	// default to standard connection string scheme
	dbUrl = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbname)
	return dbUrl, nil
}

func GetErr(key string) (string, error) {
	if value, exists := os.LookupEnv(key); exists {
		return value, nil
	}

	return "", errors.New(fmt.Sprintf("environment variable %s not set", key))
}

func ReadConfig(configFile string) kafka.ConfigMap {

	m := make(map[string]kafka.ConfigValue)

	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			before, after, found := strings.Cut(line, "=")
			if found {
				parameter := strings.TrimSpace(before)
				value := strings.TrimSpace(after)
				m[parameter] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}

	return m

}
