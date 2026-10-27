package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
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

// ответ на поиск станции
type findResponse struct {
	SessionID  int64   `json:"session_id"`
	StationID  int64   `json:"station_id,omitempty"`
	Name       string  `json:"name,omitempty"`
	DistanceKM float64 `json:"distance_km,omitempty"`
	Error      string  `json:"error,omitempty"`
}

const selectStations = `
SELECT id, name, lat, lon FROM stations
WHERE connector = $1 AND power_kw >= $2 AND status = 'free'`

func main() {
	db, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(context.Background())

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
			resp := findStation(db, d.Body)
			if resp.Error != "" {
				log.Printf("сессия %d: %s", resp.SessionID, resp.Error)
			} else {
				log.Printf("сессия %d: станция %d, %.1f км",
					resp.SessionID, resp.StationID, resp.DistanceKM)
			}

			// отвечаем, только если отправитель указал очередь для ответа
			if d.ReplyTo == "" {
				continue
			}

			body, _ := json.Marshal(resp)
			reply := amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          body,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, reply)
			cancel()
			if err != nil {
				log.Println("не удалось отправить ответ:", err)
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

// ищет ближайшую свободную станцию с нужным разъёмом и мощностью
func findStation(db *pgx.Conn, body []byte) findResponse {
	var req findRequest
	err := json.Unmarshal(body, &req)
	if err != nil {
		log.Println("не удалось разобрать сообщение:", err)
		return findResponse{Error: "неверный формат запроса"}
	}

	resp := findResponse{SessionID: req.SessionID}

	rows, err := db.Query(context.Background(), selectStations,
		req.Connector, req.PowerKW)
	if err != nil {
		log.Println("не удалось выполнить запрос:", err)
		resp.Error = "не удалось выполнить поиск"
		return resp
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		var lat, lon float64
		err := rows.Scan(&id, &name, &lat, &lon)
		if err != nil {
			log.Println("не удалось прочитать станцию:", err)
			resp.Error = "не удалось выполнить поиск"
			return resp
		}

		dist := distance(req.Lat, req.Lon, lat, lon)
		if resp.StationID == 0 || dist < resp.DistanceKM {
			resp.StationID = id
			resp.Name = name
			resp.DistanceKM = dist
		}
	}

	if resp.StationID == 0 {
		resp.Error = "подходящая станция не найдена"
		return resp
	}

	// до сотен метров
	resp.DistanceKM = math.Round(resp.DistanceKM*10) / 10
	return resp
}

// расстояние между двумя точками в километрах, формула гаверсинусов
func distance(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371 // радиус Земли, км
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * r * math.Asin(math.Sqrt(a))
}
