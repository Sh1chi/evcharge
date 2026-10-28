package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

// запрос на создание сессии, присылает gateway
type createRequest struct {
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Connector string  `json:"connector"`
	PowerKW   float64 `json:"power_kw"`
}

// ответ gateway
type createResponse struct {
	SessionID int64  `json:"session_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// запрос на поиск станции, уходит в stations
type findRequest struct {
	SessionID int64   `json:"session_id"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Connector string  `json:"connector"`
	PowerKW   float64 `json:"power_kw"`
}

// ответ stations на поиск станции
type findResponse struct {
	SessionID int64  `json:"session_id"`
	StationID int64  `json:"station_id"`
	Error     string `json:"error"`
}

const createTable = `
CREATE TABLE IF NOT EXISTS sessions (
	id         BIGSERIAL PRIMARY KEY,
	lat        DOUBLE PRECISION NOT NULL,
	lon        DOUBLE PRECISION NOT NULL,
	connector  TEXT NOT NULL,
	power_kw   DOUBLE PRECISION NOT NULL,
	status     TEXT NOT NULL,
	station_id BIGINT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

const insertSession = `
INSERT INTO sessions (lat, lon, connector, power_kw, status)
VALUES ($1, $2, $3, $4, 'searching')
RETURNING id`

const setStation = `
UPDATE sessions SET station_id = $1, status = 'assigned' WHERE id = $2`

const setNotFound = `
UPDATE sessions SET status = 'not_found' WHERE id = $1`

func main() {
	db, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(context.Background())

	_, err = db.Exec(context.Background(), createTable)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("sessions.create", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// объявляем и здесь, чтобы сообщения не пропали, если stations ещё не запущен
	_, err = ch.QueueDeclare("stations.find", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// сюда stations присылает найденную станцию
	_, err = ch.QueueDeclare("sessions.assign", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// не больше одного неподтверждённого сообщения за раз
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	found, err := ch.Consume("sessions.assign", "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// обе очереди читаются в одной горутине, подключение к базе одно
	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					return
				}
				replySession(db, ch, d)
			case d, ok := <-found:
				if !ok {
					return
				}
				assignStation(db, d.Body)
				d.Ack(false)
			}
		}
	}()

	log.Println("ожидание сообщений в очереди", q.Name)

	// ожидание SIGTERM от docker stop
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("получен сигнал завершения, сервис остановлен")
}

// создаёт сессию и отвечает gateway
func replySession(db *pgx.Conn, ch *amqp.Channel, d amqp.Delivery) {
	resp := createSession(db, ch, d.Body)

	body, _ := json.Marshal(resp)
	reply := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: d.CorrelationId,
		Body:          body,
	}

	// ответ уходит в очередь, которую gateway указал в reply_to
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, reply)
	cancel()
	if err != nil {
		log.Println("не удалось отправить ответ:", err)
	}

	d.Ack(false)
}

// записывает сессию в базу и отправляет запрос на поиск станции
func createSession(db *pgx.Conn, ch *amqp.Channel, body []byte) createResponse {
	var req createRequest
	err := json.Unmarshal(body, &req)
	if err != nil {
		log.Println("не удалось разобрать сообщение:", err)
		return createResponse{Error: "неверный формат запроса"}
	}

	var id int64
	row := db.QueryRow(context.Background(), insertSession,
		req.Lat, req.Lon, req.Connector, req.PowerKW)
	err = row.Scan(&id)
	if err != nil {
		log.Println("не удалось записать сессию:", err)
		return createResponse{Error: "не удалось создать сессию"}
	}
	log.Println("создана сессия", id)

	find := findRequest{
		SessionID: id,
		Lat:       req.Lat,
		Lon:       req.Lon,
		Connector: req.Connector,
		PowerKW:   req.PowerKW,
	}
	findBody, _ := json.Marshal(find)
	msg := amqp.Publishing{
		ContentType: "application/json",
		ReplyTo:     "sessions.assign",
		Body:        findBody,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ch.PublishWithContext(ctx, "", "stations.find", false, false, msg)
	if err != nil {
		log.Println("не удалось отправить запрос в stations:", err)
	}

	return createResponse{SessionID: id, Status: "searching"}
}

// записывает в сессию найденную станцию или статус not_found
func assignStation(db *pgx.Conn, body []byte) {
	var resp findResponse
	err := json.Unmarshal(body, &resp)
	if err != nil {
		log.Println("не удалось разобрать сообщение:", err)
		return
	}

	if resp.Error != "" {
		_, err = db.Exec(context.Background(), setNotFound, resp.SessionID)
	} else {
		_, err = db.Exec(context.Background(), setStation,
			resp.StationID, resp.SessionID)
	}
	if err != nil {
		log.Println("не удалось обновить сессию:", err)
		return
	}

	if resp.Error != "" {
		log.Printf("сессия %d: станция не найдена", resp.SessionID)
	} else {
		log.Printf("сессия %d: назначена станция %d",
			resp.SessionID, resp.StationID)
	}
}
