package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

type Event struct {
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	CreatedAt string                 `json:"created_at"`
}

type App struct {
	brokers []string
	writers map[string]*kafka.Writer
}

func main() {
	port := getEnv("PORT", "8082")
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	app := &App{
		brokers: brokers,
		writers: map[string]*kafka.Writer{
			movieTopic:   newKafkaWriter(brokers, movieTopic),
			userTopic:    newKafkaWriter(brokers, userTopic),
			paymentTopic: newKafkaWriter(brokers, paymentTopic),
		},
	}

	defer app.closeWriters()

	go app.consumeTopic(movieTopic)
	go app.consumeTopic(userTopic)
	go app.consumeTopic(paymentTopic)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", app.healthHandler)
	mux.HandleFunc("/api/events/movie", app.movieEventHandler)
	mux.HandleFunc("/api/events/user", app.userEventHandler)
	mux.HandleFunc("/api/events/payment", app.paymentEventHandler)

	log.Printf("Starting events service on port %s", port)
	log.Printf("Kafka brokers: %v", brokers)

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func newKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

func (a *App) closeWriters() {
	for _, writer := range a.writers {
		_ = writer.Close()
	}
}

func (a *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{
		"status": true,
	})
}

func (a *App) movieEventHandler(w http.ResponseWriter, r *http.Request) {
	a.eventHandler(w, r, movieTopic, "movie")
}

func (a *App) userEventHandler(w http.ResponseWriter, r *http.Request) {
	a.eventHandler(w, r, userTopic, "user")
}

func (a *App) paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	a.eventHandler(w, r, paymentTopic, "payment")
}

func (a *App) eventHandler(w http.ResponseWriter, r *http.Request, topic string, eventType string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"status": "error",
			"error":  "method not allowed",
		})
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"status": "error",
			"error":  "invalid json body",
		})
		return
	}

	event := Event{
		Type:      eventType,
		Payload:   payload,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"error":  "failed to marshal event",
		})
		return
	}

	if err := a.publishEvent(topic, eventType, eventBytes); err != nil {
		log.Printf("failed to publish %s event to Kafka topic %s: %v", eventType, topic, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"error":  "failed to publish event",
		})
		return
	}

	log.Printf("published %s event to topic %s: %s", eventType, topic, string(eventBytes))

	writeJSON(w, http.StatusCreated, map[string]string{
		"status": "success",
	})
}

func (a *App) publishEvent(topic string, key string, value []byte) error {
	writer := a.writers[topic]

	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(key),
			Value: value,
			Time:  time.Now(),
		})

		cancel()

		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("Kafka publish attempt %d failed for topic %s: %v", attempt, topic, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return lastErr
}

func (a *App) consumeTopic(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        a.brokers,
		Topic:          topic,
		GroupID:        "cinemaabyss-events-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	defer reader.Close()

	log.Printf("Kafka consumer started for topic %s", topic)

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka consumer error for topic %s: %v", topic, err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf(
			"processed event from Kafka topic=%s partition=%d offset=%d key=%s value=%s",
			message.Topic,
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
		)
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(body)
}