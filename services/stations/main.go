package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

// запрос на поиск станции, присылает sessions
type findRequest struct {
	SessionID int64   `json:"session_id"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Connector string  `json:"connector"`
	PowerKW   float64 `json:"power_kw"`
}

func main() {
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

	// durable, чтобы очередь пережила перезапуск RabbitMQ
	q, err := ch.QueueDeclare("stations.find", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for d := range msgs {
			var req findRequest
			err := json.Unmarshal(d.Body, &req)
			if err != nil {
				log.Println("не удалось разобрать сообщение:", err)
				continue
			}
			log.Printf("поиск станции для сессии %d: %.4f, %.4f, %s, %.0f кВт",
				req.SessionID, req.Lat, req.Lon, req.Connector, req.PowerKW)
		}
	}()

	log.Println("ожидание сообщений в очереди", q.Name)

	// ожидание SIGTERM от docker stop
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("получен сигнал завершения, сервис остановлен")
}
