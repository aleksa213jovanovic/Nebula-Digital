package main

import (
	"bufio"
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

	go setConsumer(db)

	setRouter(db)
}

func setRouter(db *sql.DB) error {

	port := "8082"
	// todo
	// port, err := GetErr("APP_PORT")
	// if err != nil {
	// 	return err
	// }

	server := http.Server{
		Addr: fmt.Sprintf(":%s", port),
	}

	handler := handler{
		db: db,
	}

	http.HandleFunc("/transaction-service/fetch/user", handler.fetchUser)

	log.Printf("Starting server on :%s\n", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("%v\n", err)
		return err
	}

	// todo
	// go func() {
	// 	log.Println("Starting server on :8082")
	// 	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
	// 		log.Fatalf("Could not listen on :8082: %v\n", err)
	// 	}
	// }()

	return nil
}

type handler struct {
	db *sql.DB
}

type User struct {
	Email  string `json:"email"`
	UserId int    `json:"userId"`
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

	// Set Content-Type header and write the JSON response
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

func setConsumer(db *sql.DB) {
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
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
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
	return "postgres://user:password@localhost:5438/transactionservice?sslmode=disable", nil

	// todo docker
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
