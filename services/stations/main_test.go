package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// тесты запускаются с компьютера, на котором подняты контейнеры
const amqpURL = "amqp://evcharge:evcharge@localhost:5672/"

// точка в центре Барнаула, от неё считаются расстояния
const lat = 53.3474
const lon = 83.7784

var conn *amqp.Connection

// одно подключение к RabbitMQ на все тесты
func TestMain(m *testing.M) {
	var err error
	conn, err = amqp.Dial(amqpURL)
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	conn.Close()
	os.Exit(code)
}

// отправляет запрос в stations.find и ждёт ответа
func call(t *testing.T, body []byte) findResponse {
	var resp findResponse

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	// временная очередь для ответа, удаляется вместе с каналом
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	corrID := strconv.FormatInt(time.Now().UnixNano(), 10)
	msg := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       q.Name,
		Body:          body,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx, "", "stations.find", false, false, msg)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case d := <-msgs:
		if d.CorrelationId != corrID {
			t.Fatal("ответ на чужой запрос:", d.CorrelationId)
		}
		err = json.Unmarshal(d.Body, &resp)
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("stations не ответил за 5 секунд")
	}
	return resp
}

// запрос на поиск станции из центра, сессии 0 не бывает
func find(t *testing.T, connector string, power float64) findResponse {
	req := findRequest{
		SessionID: 0,
		Lat:       lat,
		Lon:       lon,
		Connector: connector,
		PowerKW:   power,
	}
	body, _ := json.Marshal(req)
	return call(t, body)
}

// CCS2 есть у станций 1 и 2, станция 1 ближе
func TestNearest(t *testing.T) {
	resp := find(t, "CCS2", 50)
	if resp.StationID != 1 {
		t.Errorf("ожидалась станция 1, получено %+v", resp)
	}
}

// Type2 есть у станций 3 и 4, ближняя станция 3 занята
func TestBusySkipped(t *testing.T) {
	resp := find(t, "Type2", 22)
	if resp.StationID != 4 {
		t.Errorf("ожидалась станция 4, получено %+v", resp)
	}
}

// CHAdeMO есть у станций 5 и 6, у ближней станции 5 только 50 кВт
func TestLowPowerSkipped(t *testing.T) {
	resp := find(t, "CHAdeMO", 100)
	if resp.StationID != 6 {
		t.Errorf("ожидалась станция 6, получено %+v", resp)
	}
}

// станций с разъёмом GB/T нет
func TestUnknownConnector(t *testing.T) {
	resp := find(t, "GB/T", 50)
	if resp.StationID != 0 || resp.Error == "" {
		t.Errorf("ожидалась ошибка, получено %+v", resp)
	}
}

// в теле обрезанный JSON
func TestBadMessage(t *testing.T) {
	resp := call(t, []byte("{"))
	if resp.Error != "неверный формат запроса" {
		t.Errorf("ожидалась ошибка формата, получено %+v", resp)
	}
}
