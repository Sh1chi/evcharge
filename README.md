# evcharge

Сервис для сети зарядных станций электромобилей. Сервисы на Go,
общаются через RabbitMQ:

- gateway - HTTP-вход
- sessions - сессии зарядки, хранит их в PostgreSQL
- stations - станции

## Запуск

```
docker compose up --build -d
```

Веб-интерфейс RabbitMQ: `http://localhost:15672`, логин и пароль `evcharge`
