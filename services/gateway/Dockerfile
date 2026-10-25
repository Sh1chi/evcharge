# --- 1. Сборка
FROM golang:1.23-alpine AS builder

WORKDIR /src

# go.mod и go.sum отдельным слоем, чтобы правка кода не сбрасывала кэш зависимостей
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# отключает связывание с библиотеками C
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/gateway ./services/gateway

# --- 2. Итоговый образ
FROM scratch

COPY --from=builder /out/gateway /gateway

# числом, потому что в пустом образе нет /etc/passwd
USER 65534:65534

EXPOSE 8080

# списком, чтобы программа получала SIGTERM напрямую, а не через оболочку
ENTRYPOINT ["/gateway"]
