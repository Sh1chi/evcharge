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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// тело запроса POST /api/v1/sessions
type createRequest struct {
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Connector string  `json:"connector"`
	PowerKW   float64 `json:"power_kw"`
}

// запрос в sessions.get и sessions.finish
type idRequest struct {
	SessionID int64 `json:"session_id"`
}

// запрос в stations.list
type listRequest struct {
	Connector string `json:"connector"`
}

// запрос в stations.status
type statusRequest struct {
	StationID int64  `json:"station_id"`
	Status    string `json:"status"`
}

// из ответа сервиса gateway читает только поле error
type serviceReply struct {
	Error string `json:"error"`
}

// заголовки сообщения AMQP, через них передаётся контекст трейса
type headersCarrier amqp.Table

func (c headersCarrier) Get(key string) string {
	value, _ := c[key].(string)
	return value
}

func (c headersCarrier) Set(key, value string) {
	c[key] = value
}

func (c headersCarrier) Keys() []string {
	keys := []string{}
	for key := range c {
		keys = append(keys, key)
	}
	return keys
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

	body, err := call(r.Context(), "sessions.create", req)
	if err != nil {
		log.Println("нет ответа от sessions:", err)
		http.Error(w, "сервис сессий не отвечает", http.StatusGatewayTimeout)
		return
	}
	writeReply(w, body, http.StatusCreated, http.StatusInternalServerError)
}

// GET /api/v1/sessions/{id}
func getSessionHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "неверный номер сессии", http.StatusBadRequest)
		return
	}

	body, err := call(r.Context(), "sessions.get", idRequest{SessionID: id})
	if err != nil {
		log.Println("нет ответа от sessions:", err)
		http.Error(w, "сервис сессий не отвечает", http.StatusGatewayTimeout)
		return
	}
	writeReply(w, body, http.StatusOK, http.StatusNotFound)
}

// POST /api/v1/sessions/{id}/finish
func finishSessionHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "неверный номер сессии", http.StatusBadRequest)
		return
	}

	body, err := call(r.Context(), "sessions.finish", idRequest{SessionID: id})
	if err != nil {
		log.Println("нет ответа от sessions:", err)
		http.Error(w, "сервис сессий не отвечает", http.StatusGatewayTimeout)
		return
	}
	writeReply(w, body, http.StatusOK, http.StatusConflict)
}

// GET /api/v1/stations?connector=CCS2
func listStationsHandler(w http.ResponseWriter, r *http.Request) {
	req := listRequest{Connector: r.URL.Query().Get("connector")}

	body, err := call(r.Context(), "stations.list", req)
	if err != nil {
		log.Println("нет ответа от stations:", err)
		http.Error(w, "сервис станций не отвечает", http.StatusGatewayTimeout)
		return
	}
	writeReply(w, body, http.StatusOK, http.StatusInternalServerError)
}

// PATCH /api/v1/stations/{id}, тело {"status": "free"}
func setStatusHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "неверный номер станции", http.StatusBadRequest)
		return
	}

	var req statusRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "неверный формат запроса", http.StatusBadRequest)
		return
	}
	req.StationID = id

	body, err := call(r.Context(), "stations.status", req)
	if err != nil {
		log.Println("нет ответа от stations:", err)
		http.Error(w, "сервис станций не отвечает", http.StatusGatewayTimeout)
		return
	}
	writeReply(w, body, http.StatusOK, http.StatusBadRequest)
}

// отдаёт клиенту ответ сервиса, код зависит от поля error
func writeReply(w http.ResponseWriter, body []byte, okCode, errCode int) {
	var reply serviceReply
	json.Unmarshal(body, &reply)

	w.Header().Set("Content-Type", "application/json")
	if reply.Error != "" {
		w.WriteHeader(errCode)
	} else {
		w.WriteHeader(okCode)
	}
	w.Write(body)
}

// отправляет запрос в очередь queue и ждёт ответа
func call(ctx context.Context, queue string, req any) ([]byte, error) {
	ctx, span := otel.Tracer("gateway").Start(ctx, queue+" send",
		trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	// временная очередь для ответа, удаляется вместе с каналом
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	// контекст трейса уходит в заголовках сообщения
	headers := amqp.Table{}
	otel.GetTextMapPropagator().Inject(ctx, headersCarrier(headers))

	corrID := strconv.FormatInt(time.Now().UnixNano(), 10)
	body, _ := json.Marshal(req)
	msg := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       q.Name,
		Headers:       headers,
		Body:          body,
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx, "", queue, false, false, msg)
	if err != nil {
		return nil, err
	}
	log.Println("запрос отправлен в", queue, "ожидание ответа")

	for {
		select {
		case d := <-msgs:
			if d.CorrelationId != corrID {
				continue
			}
			return d.Body, nil
		case <-ctx.Done():
			span.SetStatus(codes.Error, "нет ответа")
			return nil, ctx.Err()
		}
	}
}

// имя спана - маршрут, например GET /api/v1/sessions/{id}
func spanName(operation string, r *http.Request) string {
	return r.Pattern
}

func main() {
	// адрес Jaeger и имя сервиса берутся из переменных OTEL_ в compose
	exp, err := otlptracehttp.New(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	conn, err = amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	http.HandleFunc("POST /api/v1/sessions", createSessionHandler)
	http.HandleFunc("GET /api/v1/sessions/{id}", getSessionHandler)
	http.HandleFunc("POST /api/v1/sessions/{id}/finish", finishSessionHandler)
	http.HandleFunc("GET /api/v1/stations", listStationsHandler)
	http.HandleFunc("PATCH /api/v1/stations/{id}", setStatusHandler)

	// спан на каждый HTTP-запрос
	handler := otelhttp.NewHandler(http.DefaultServeMux, "gateway",
		otelhttp.WithSpanNameFormatter(spanName))
	srv := &http.Server{Addr: ":8080", Handler: handler}

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

	// отправляет в Jaeger спаны, которые ещё не ушли
	tp.Shutdown(context.Background())
	log.Println("сервер остановлен")
}
