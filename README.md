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

Создать сессию зарядки:

```
POST http://localhost:8080/api/v1/sessions

{"lat": 53.3474, "lon": 83.7784, "connector": "CCS2", "power_kw": 50}
```

Веб-интерфейс RabbitMQ: `http://localhost:15672`, логин и пароль `evcharge`
