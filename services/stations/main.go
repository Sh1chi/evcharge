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

// запрос на список станций, пустой разъём - все станции
type listRequest struct {
	Connector string `json:"connector"`
}

type station struct {
	ID        int64   `json:"id"`
	Name      string  `json:"name"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Connector string  `json:"connector"`
	PowerKW   float64 `json:"power_kw"`
	Status    string  `json:"status"`
}

type listResponse struct {
	Stations []station `json:"stations"`
	Error    string    `json:"error,omitempty"`
}

// запрос на смену статуса станции
type statusRequest struct {
	StationID int64  `json:"station_id"`
	Status    string `json:"status"`
}

type statusResponse struct {
	StationID int64  `json:"station_id"`
	Status    string `json:"status,omitempty"`
	Error     string `json:"error,omitempty"`
}

const selectStations = `
SELECT id, name, lat, lon FROM stations
WHERE connector = $1 AND power_kw >= $2 AND status = 'free'`

const selectAll = `
SELECT id, name, lat, lon, connector, power_kw, status FROM stations
WHERE $1 = '' OR connector = $1
ORDER BY id`

const updateStatus = `
UPDATE stations SET status = $1 WHERE id = $2`

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

	_, err = ch.QueueDeclare("stations.list", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = ch.QueueDeclare("stations.status", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	lists, err := ch.Consume("stations.list", "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	statuses, err := ch.Consume("stations.status", "",
		true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// все очереди читаются в одной горутине, подключение к базе одно
	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					return
				}
				resp := findStation(db, d.Body)
				if resp.Error != "" {
					log.Printf("сессия %d: %s", resp.SessionID, resp.Error)
				} else {
					log.Printf("сессия %d: станция %d, %.1f км",
						resp.SessionID, resp.StationID, resp.DistanceKM)
				}
				reply(ch, d, resp)
			case d, ok := <-lists:
				if !ok {
					return
				}
				reply(ch, d, listStations(db, d.Body))
			case d, ok := <-statuses:
				if !ok {
					return
				}
				reply(ch, d, setStatus(db, d.Body))
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

// отправляет ответ в очередь из reply_to
func reply(ch *amqp.Channel, d amqp.Delivery, resp any) {
	// отвечаем, только если отправитель указал очередь для ответа
	if d.ReplyTo == "" {
		return
	}

	body, _ := json.Marshal(resp)
	msg := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: d.CorrelationId,
		Body:          body,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, msg)
	if err != nil {
		log.Println("не удалось отправить ответ:", err)
	}
}

// возвращает станции с нужным разъёмом или все
func listStations(db *pgx.Conn, body []byte) listResponse {
	var req listRequest
	err := json.Unmarshal(body, &req)
	if err != nil {
		log.Println("не удалось разобрать сообщение:", err)
		return listResponse{Error: "неверный формат запроса"}
	}

	// пустой срез, чтобы в JSON был [], а не null
	resp := listResponse{Stations: []station{}}

	rows, err := db.Query(context.Background(), selectAll, req.Connector)
	if err != nil {
		log.Println("не удалось выполнить запрос:", err)
		resp.Error = "не удалось получить станции"
		return resp
	}
	defer rows.Close()

	for rows.Next() {
		var s station
		err := rows.Scan(&s.ID, &s.Name, &s.Lat, &s.Lon,
			&s.Connector, &s.PowerKW, &s.Status)
		if err != nil {
			log.Println("не удалось прочитать станцию:", err)
			resp.Error = "не удалось получить станции"
			return resp
		}
		resp.Stations = append(resp.Stations, s)
	}
	return resp
}

// меняет статус станции на free или busy
func setStatus(db *pgx.Conn, body []byte) statusResponse {
	var req statusRequest
	err := json.Unmarshal(body, &req)
	if err != nil {
		log.Println("не удалось разобрать сообщение:", err)
		return statusResponse{Error: "неверный формат запроса"}
	}

	resp := statusResponse{StationID: req.StationID}

	if req.Status != "free" && req.Status != "busy" {
		resp.Error = "статус должен быть free или busy"
		return resp
	}

	tag, err := db.Exec(context.Background(), updateStatus,
		req.Status, req.StationID)
	if err != nil {
		log.Println("не удалось обновить станцию:", err)
		resp.Error = "не удалось обновить станцию"
		return resp
	}
	if tag.RowsAffected() == 0 {
		resp.Error = "станция не найдена"
		return resp
	}

	resp.Status = req.Status
	log.Printf("станция %d: статус %s", req.StationID, req.Status)
	return resp
}
