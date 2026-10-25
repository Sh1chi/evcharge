package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// тело запроса POST /api/v1/sessions
type createRequest struct {
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Connector string  `json:"connector"`
	PowerKW   float64 `json:"power_kw"`
}

// ответ sessions
type createResponse struct {
	SessionID int64  `json:"session_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

var conn *amqp.Connection

// POST /api/v1/sessions
func createSessionHandler(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "неверный формат запроса", http.StatusBadRequest)
		return
	}

	resp, err := callSessions(req)
	if err != nil {
		log.Println("нет ответа от sessions:", err)
		http.Error(w, "сервис сессий не отвечает", http.StatusGatewayTimeout)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if resp.Error != "" {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	json.NewEncoder(w).Encode(resp)
}

// отправляет запрос в sessions.create и ждёт ответа
func callSessions(req createRequest) (createResponse, error) {
	var resp createResponse

	ch, err := conn.Channel()
	if err != nil {
		return resp, err
	}
	defer ch.Close()

	// временная очередь для ответа, удаляется вместе с каналом
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return resp, err
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return resp, err
	}

	corrID := strconv.FormatInt(time.Now().UnixNano(), 10)
	body, _ := json.Marshal(req)
	msg := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       q.Name,
		Body:          body,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx, "", "sessions.create", false, false, msg)
	if err != nil {
		return resp, err
	}
	log.Println("запрос отправлен в sessions.create, ожидание ответа")

	for {
		select {
		case d := <-msgs:
			if d.CorrelationId != corrID {
				continue
			}
			err = json.Unmarshal(d.Body, &resp)
			return resp, err
		case <-ctx.Done():
			return resp, ctx.Err()
		}
	}
}

func main() {
	var err error
	conn, err = amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	http.HandleFunc("POST /api/v1/sessions", createSessionHandler)

	srv := &http.Server{Addr: ":8080"}

	// ListenAndServe не возвращает управление, пока сервер работает
	go func() {
		log.Println("сервер запущен на :8080")
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// ожидание SIGTERM от docker stop
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("получен сигнал завершения, закрываем сервер")
	srv.Shutdown(context.Background())
	log.Println("сервер остановлен")
}
