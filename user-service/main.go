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
	"runtime/debug"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	fmt.Printf("RUNNING USER SERVICE\n")

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

	// _ = db.QueryRow("insert into users (email) values ($1)", "test@email")

	conf := ReadConfig("./kafka.server.properties")
	err = conf.SetKey("enable.idempotence", true)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	p, err := kafka.NewProducer(&conf)

	if err != nil {
		log.Fatalf("failed to create kafka producer, %v\n", err)
	}

	defer p.Flush(15 * 1000) // todo check this
	defer p.Close()

	// Wait for all messages to be delivered

	err = setRouter(db, p)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
}

func setRouter(db *sql.DB, p *kafka.Producer) error {

	port := "8083"
	// todo
	// port, err := GetErr("APP_PORT")
	// if err != nil {
	// 	return err
	// }

	server := http.Server{
		Addr: fmt.Sprintf(":%s", port),
	}

	handler := handler{
		db:            db,
		kafkaProducer: p,
	}

	http.HandleFunc("/user-service/create/user", handler.createUserHandler)

	log.Printf("Starting server on :%s\n", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	db            *sql.DB
	kafkaProducer *kafka.Producer
}

type CreateUserRequest struct {
	Email string `json:"email"`
}

type User struct {
	Email  string `json:"email"`
	UserId int    `json:"userId"`
}

func (h *handler) createUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	var req CreateUserRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Extracted email
	email := req.Email

	userId, err := InsertUser(h.db, req)
	if err != nil {
		if err.Error() == "user already exists" {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		} else {
			http.Error(w, "fail", http.StatusInternalServerError)
			return
		}
	}

	newUser := &User{
		Email:  req.Email,
		UserId: userId,
	}

	payload, err := json.Marshal(newUser)
	if err != nil {
		http.Error(w, "fail", http.StatusInternalServerError)
		return
	}

	topic := "userCreate"

	kafkaSucceeded := make(chan bool)
	kafkaKey := fmt.Sprintf("userId.%d", userId)

	err = h.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(kafkaKey),
		Value:          payload,
	}, nil)

	if err != nil {
		RemoveUser(userId, h.db)
		fmt.Printf("%v\n", err)
		http.Error(w, "fail", http.StatusInternalServerError)
	}

	go func() {
		for e := range h.kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					if string(ev.Key) == kafkaKey {
						fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
						kafkaSucceeded <- false
						return
					}
				} else {
					if string(ev.Key) == kafkaKey {
						fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
							*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
						kafkaSucceeded <- true
						return
					}
				}
			}
		}
	}()

	success := <-kafkaSucceeded
	if success {
		fmt.Printf("user created successfully with email %s", email)
		w.WriteHeader(http.StatusOK)
	} else {
		RemoveUser(userId, h.db)
		http.Error(w, "fail", http.StatusInternalServerError)
	}

	// todo check if new user is recorded in transaction service
}

func InsertUser(db *sql.DB, user CreateUserRequest) (int, error) {
	// SQL statement to insert a new user
	query := `INSERT INTO users (email) VALUES ($1) RETURNING user_id`

	var userID int
	err := db.QueryRow(query, user.Email).Scan(&userID)
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

func RemoveUser(userID int, db *sql.DB) {
	// SQL statement to delete a user
	query := `DELETE FROM users WHERE user_id = $1`

	// Execute the SQL statement
	db.Exec(query, userID)
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

func buildDBUrl() (string, error) {
	return "postgres://user:password@localhost:5437/userservice?sslmode=disable", nil

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
