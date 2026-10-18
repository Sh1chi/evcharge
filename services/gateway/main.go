package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type helloResponse struct {
	Message string `json:"message"`
}

// GET /hello, параметр name необязательный
func helloHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "World"
	}

	resp := helloResponse{Message: "Hello, " + name + "!"}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func main() {
	http.HandleFunc("/hello", helloHandler)

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
