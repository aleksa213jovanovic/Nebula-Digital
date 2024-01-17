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
	"github.com/nats-io/nats.go"

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

	fmt.Printf("connecting to kafka server\n")

	conf := ReadConfig("./kafka.server.properties")
	err = conf.SetKey("enable.idempotence", true)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	p, err := kafka.NewProducer(&conf)

	if err != nil {
		log.Fatalf("failed to create kafka producer, %v\n", err)
	}

	defer p.Flush(15 * 1000)
	defer p.Close()

	fmt.Printf("succesfully connected to kafka\n")

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

	fmt.Printf("succesfully connected to ants\n")

	// Wait for all messages to be delivered

	err = setRouter(db, p, nc)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
}

func setRouter(db *sql.DB, p *kafka.Producer, nc *nats.Conn) error {

	// port := "8082"
	port, err := GetErr("APP_PORT")
	if err != nil {
		return err
	}

	server := http.Server{
		Addr: fmt.Sprintf(":%s", port),
	}

	handler := handler{
		db:            db,
		kafkaProducer: p,
		nats:          nc,
	}

	http.HandleFunc("/user-service/user/create", handler.createUserHandler)
	http.HandleFunc("/user-service/user/balance", handler.userBalanceHandler)

	log.Printf("Starting server on :%s\n", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (h *handler) userBalanceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	type requestBody struct {
		Email string `json:"email"`
	}

	req := requestBody{}

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	email := req.Email

	userBalance, err := fetchUserBalance(h.db, h.nats, email)
	if err != nil {
		http.Error(w, "fail", http.StatusInternalServerError)
		return
	}

	type responseBody struct {
		Email   string  `json:"email"`
		Balance float64 `json:"balance"`
	}

	res := &responseBody{
		Email:   email,
		Balance: userBalance,
	}

	payload, err := json.Marshal(res)
	if err != nil {
		http.Error(w, "fail", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)

}

func fetchUserBalance(db *sql.DB, nc *nats.Conn, email string) (float64, error) {
	// send request to nats io
	// return response in http response body

	fmt.Printf("fetching userid\n")

	query := fmt.Sprintf("SELECT user_id FROM users WHERE email='%s'", email)

	var userId int
	err := db.QueryRow(query).Scan(&userId)
	if err != nil {
		return 0, err
	}

	fmt.Printf("user id = %d\n", userId)

	type request struct {
		UserId int `json:"userId"`
	}

	req := &request{
		userId,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("%v\n", err)
		return 0, err
	}

	natsResponse, err := nc.Request("userBalance", payload, 5*time.Second)
	if err != nil {
		fmt.Printf("%v\n", err)
		return 0, err
	}

	type response struct {
		UserBalance float64 `json:"userBalance"`
	}

	resp := response{}

	err = json.Unmarshal(natsResponse.Data, &resp)
	if err != nil {
		fmt.Printf("%v\n", err)
		return 0, err
	}

	return resp.UserBalance, nil
}

type handler struct {
	db            *sql.DB
	kafkaProducer *kafka.Producer
	nats          *nats.Conn
}

func (h *handler) createUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	type createUserRequest struct {
		Email string `json:"email"`
	}

	var req createUserRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Extracted email
	email := req.Email

	userId, err := InsertUser(h.db, req.Email)
	if err != nil {
		if err.Error() == "user already exists" {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		} else {
			http.Error(w, "fail", http.StatusInternalServerError)
			return
		}
	}

	type createUserResponse struct {
		Email  string `json:"email"`
		UserId int    `json:"user_id"`
	}

	newUser := &createUserResponse{
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
		return
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

	close(kafkaSucceeded)
}

func InsertUser(db *sql.DB, userEmail string) (int, error) {
	// SQL statement to insert a new userEmail
	query := `INSERT INTO users (email) VALUES ($1) RETURNING user_id`

	var userID int
	err := db.QueryRow(query, userEmail).Scan(&userID)
	if err != nil {
		// Check if the error is due to a duplicate entry
		if pqErr, ok := err.(*pq.Error); ok {
			// Here "23505" is the PostgreSQL error code for "unique_violation"
			if pqErr.Code == "23505" {
				return -1, fmt.Errorf("user email already exists")
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
	// return "postgresql://user:password@localhost:5437/userservice?sslmode=disable", nil

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
