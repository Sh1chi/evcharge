# evcharge

Сервис `gateway` на Go, упакованный в Docker-образ.
Отвечает на `GET /hello`, параметр `name` необязательный.

| Запрос | Ответ |
|---|---|
| `/hello` | `{"message":"Hello, World!"}` |
| `/hello?name=Artem` | `{"message":"Hello, Artem!"}` |

## Запуск

```powershell
docker build -f services/gateway/Dockerfile -t evcharge/gateway:1.0 .
docker run -d --name evcharge-gateway -p 8080:8080 evcharge/gateway:1.0
```

Проверить: открыть в браузере `http://localhost:8080/hello`.

```powershell
docker logs evcharge-gateway
docker stop evcharge-gateway
docker rm evcharge-gateway
```

Или через compose:

```powershell
docker compose up --build -d
docker compose down
```

Без Docker: `go run ./services/gateway`.

## Структура

```
services/gateway/   код сервиса и Dockerfile
```

Итоговый образ занимает 12,2 МБ на диске против 370 МБ у образа с
компилятором: сборка двухэтапная, в готовый образ попадает только
скомпилированный файл.
