# МКК Базис: асинхронный процессинг платежей

FastAPI принимает платёж и Outbox-событие в одной транзакции PostgreSQL. Фоновый
relay публикует подтверждённое сообщение в RabbitMQ. Один FastStream consumer
эмулирует платёж, сохраняет результат и доставляет webhook с общим бюджетом в три
прикладные попытки.

## Запуск

Нужны Docker Engine и **Docker Compose V2 или новее** (`docker compose`). Python на
хосте для запуска сервиса не нужен. Образы и Python-зависимости закреплены версиями
и `uv.lock`; Python 3.13, PostgreSQL 18.6, RabbitMQ 4.4.0.

```bash
cp .env.example .env
docker compose --profile demo up -d --build --wait
docker compose ps
```

Основных сервисов четыре: `postgres`, `rabbitmq`, `api`, `consumer`. Профиль `demo`
добавляет локальный webhook-приёмник. API доступен на `http://127.0.0.1:8000`,
приёмник — на `http://127.0.0.1:8001`. Порты PostgreSQL и RabbitMQ наружу не открыты.
Конфликтующие порты меняются через `API_PORT` и `DEMO_PORT` в `.env`.

В `.env.example` только демонстрационные ключи. Для собственного окружения замените
их в `.env`; API_KEY должен содержать минимум 16 символов. Пароли PostgreSQL/RabbitMQ
в этом Compose входят в URL, поэтому используйте URL-safe символы, например hex.
API выполняет `alembic upgrade head` перед Uvicorn; consumer стартует после здоровых
API и RabbitMQ. Healthcheck API проверяет только PostgreSQL и наличие схемы.

## Создание и получение платежа

Примеры используют ключ из `.env.example`. Если вы его заменили, измените заголовок.

```bash
curl -i http://127.0.0.1:8000/api/v1/payments \
  -H 'X-API-Key: local-development-api-key-change-me' \
  -H 'Idempotency-Key: order-1001' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Оплата заказа 1001",
    "metadata": {"order_id": 1001},
    "webhook_url": "http://webhook-receiver:8000/webhook"
  }'
```

Ответ `202 Accepted`:

```json
{"payment_id":"<uuid>","status":"pending","created_at":"2026-11-10T16:00:00Z"}
```

```bash
curl http://127.0.0.1:8000/api/v1/payments/UUID_ИЗ_ОТВЕТА \
  -H 'X-API-Key: local-development-api-key-change-me'
curl http://127.0.0.1:8001/events
curl http://127.0.0.1:8000/health \
  -H 'X-API-Key: local-development-api-key-change-me'
```

GET возвращает поля платежа, `processed_at`, `processing_attempts`,
`processing_exhausted` и `webhook_delivered_at`. Суммы в JSON — строки; даты — UTC.
Платёж со статусом `succeeded` может иметь `processing_exhausted=true`: платёж
завершился, а уведомление доставить не удалось. Бизнес-результат `failed` также
доставляется webhook и сам по себе не является причиной DLQ.

Повтор POST с тем же ключом и нормализованным телом возвращает прежний ID и актуальный
статус. Изменение тела даёт `409`. Уникальность обеспечивает ограничение PostgreSQL,
в том числе для конкурентных запросов. TTL ключей отсутствует.

| Код | Значение |
|---|---|
| 202 | Создан или найден тот же платёж |
| 401 | API-ключ отсутствует или неверен |
| 404 | Платёж не найден |
| 409 | Ключ уже использован с другим телом |
| 422 | Ошибка суммы, валюты, URL, метаданных или обязательного заголовка |
| 503 | PostgreSQL временно недоступен |

Все маршруты основного API, включая `/health`, требуют ключ; docs/OpenAPI отключены.
Метаданные — JSON-объект, сумма положительная, максимум `9999999999999999.99`,
не более двух значащих десятичных разрядов. Валюты: RUB, USD, EUR.
`amount` принимается и JSON-числом, и строкой: десятичная точность сохраняется при
разборе исходного тела запроса. NUL, некорректный Unicode, NaN/Infinity и лишние
десятичные разряды дают 422. Metadata использует JSON int/finite float.

## Webhook и повторы

Consumer ждёт 2–5 секунд. Результат определяется хешем UUID платежа: примерно 90%
`succeeded` и 10% `failed`, воспроизводимо при повторной обработке того же ID.

```json
{
  "event_id": "<стабильный uuid>",
  "event_type": "payment.completed",
  "payment_id": "<uuid>",
  "status": "succeeded",
  "amount": "1500.50",
  "currency": "RUB",
  "processed_at": "2026-11-10T16:00:03+00:00"
}
```

HTTP POST отправляется с `Idempotency-Key: <event_id>`. Общий таймаут — 5 секунд,
редиректы отключены, любой 2xx означает доставку. Входной API-ключ не пересылается.
При неопределённом исходе HTTP или падении до сохранения подтверждения повтор возможен;
получатель должен дедуплицировать по `event_id`.

Три начатые прикладные попытки включают первую; задержки после ошибок — 1 и 2 секунды.
Счётчик фиксируется до побочных эффектов, падение процесса расходует начатую попытку.
Разрыв AMQP в живом процессе сохраняет управляемую задачу: повторная доставка
присоединяется к той же попытке. При shutdown bootstrap отменяет и ожидает эту задачу.
Повтор продолжает незавершённую стадию: сохранённый результат шлюза не вычисляется
заново. Недоступность БД/брокера ожидается без новых попыток и ACK. После исчерпания
сохраняется состояние и выполняется reject без requeue в DLQ. Некорректное событие
попадает туда сразу. Доставленный webhook проверяется до бюджета и повторно даёт ACK.
Ошибки JSON/UTF-8 и слишком глубокая вложенность сообщения также ведут в DLQ,
освобождая очередь для следующего платежа.

Проверить ошибки демонстрационного приёмника можно разными ключами POST и URL:

- `http://webhook-receiver:8000/webhook?failures=2` — 500, 500, 200;
- `http://webhook-receiver:8000/webhook?status=500` — три ошибки и DLQ.

`GET :8001/events` показывает число обращений и принятые события; память приёмника
очищается при его рестарте. Это вспомогательный локальный сервис без аутентификации.
Для другого получателя добавьте точный origin (схема, host, port) в JSON-массив
`WEBHOOK_ALLOWED_ORIGINS` и пересоздайте API/consumer. Разрешённые origins задаёт
оператор; они должны указывать на доверенные узлы/DNS. Политика не ограничивает путь
внутри разрешённого origin.

## Архитектура и гарантии

```text
src/mkk_basis/
  domain/        # dataclasses, деньги, статусы, переход результата
  application/   # use cases и Protocol-порты; только stdlib и domain
  adapters/      # FastAPI/Pydantic, SQLAlchemy, FastStream, HTTPX
  bootstrap/     # настройки, DI, lifecycle, demo receiver
migrations/      # Alembic: payments, outbox и токен попытки
tests/           # fake-порты, HTTP и реальные PostgreSQL/RabbitMQ
scripts/         # проверка восстановления после отказов
```

Outbox relay живёт в процессе consumer. Он захватывает запись через `SKIP LOCKED`
короткой транзакцией, сохраняет token/lease на 30 секунд, публикует вне транзакции
и отмечает результат новой транзакцией только при совпадении token. Публикация
требует положительного confirm и отсутствия mandatory return. Сбой после confirm
допускает повтор с прежним `message_id`; потеря события не маскируется отметкой успеха.
Повторы Outbox не ограничены тремя, backoff растёт до 30 секунд. POST работает при
выключенном RabbitMQ. Неопубликованные записи остаются в PostgreSQL.

Топология: durable direct exchange `payments` → routing key `payments.new` → quorum
queue `payments.new`; DLX `payments.dlx` → routing key `payments.failed` → quorum queue
`payments.dlq`. Persistent-сообщения, manual ACK и prefetch=1. Для исходной очереди
включены `dead-letter-strategy=at-least-once` и `overflow=reject-publish`.

## Проверки

Для локальной разработки нужны Python 3.13 и uv 0.13.7:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run lint-imports
uv run pytest -q -m 'not integration'
```

Интеграционные тесты запускаются в **отдельном Compose-проекте**. Профиль `test`
поднимает собственные `test-postgres` и `test-rabbitmq` с отдельными volumes,
БД/vhost `mkk_basis_test` и тестовым пользователем. `tests` не наследует URL основного
приложения. До миграций, TRUNCATE и purge проверяются маркер окружения и точные
назначения обоих URL; подключение к обычным `postgres`/`rabbitmq` отклоняется.

```bash
docker compose --env-file .env.example -p mkk-basis-tests --profile test run --build --rm tests
docker compose --env-file .env.example -p mkk-basis-tests --profile test down --volumes
```

Полная приёмка на новых volumes с остановкой сервисов, настоящими HTTP-запросами,
эмулятором и проверкой восстановления:

```bash
python3 scripts/acceptance.py
```

Скрипт создаёт отдельный проект с UUID и портами 18080/18081, использует только локальные
настройки `.env.example`, в конце удаляет свои контейнеры и volumes. Можно выбрать
порты через `--api-port`/`--demo-port`. Нужны Docker и Python 3.10+ на хосте.
Остановки БД и consumer выполняются после подтверждения незавершённого первого
webhook через управляемый барьер демо-приёмника. Обещание восстановления без рестарта
проверяется по container ID, StartedAt и RestartCount; нарушение предусловий завершает
сценарий ошибкой. Барьеры `/gates/{name}` доступны только у вспомогательного demo-сервиса.

## Эксплуатация локального стенда

```bash
docker compose logs -f api consumer
docker compose exec rabbitmq rabbitmqctl list_queues name type messages_ready messages_unacknowledged
docker compose exec rabbitmq rabbitmqctl list_bindings source_name destination_name routing_key
docker compose exec postgres psql -U payments -d payments \
  -c 'SELECT id, status, processing_attempts, exhausted_at, webhook_delivered_at FROM payments;'
docker compose exec postgres psql -U payments -d payments \
  -c 'SELECT id, published_at, attempts, next_attempt_at FROM outbox;'
docker compose --profile demo down
```

`down` сохраняет данные. Добавляйте `--volumes` только для намеренного удаления данных
этого стенда. Автоматического replay из DLQ и удаления старых Outbox-записей нет;
оператор изучает причину и определяет дальнейшее действие. Одноузловой RabbitMQ в
Compose проверяет долговечность и reconnect, но не отказоустойчивость к потере узла
или диска. Для внешнего платёжного шлюза нужен его собственный идемпотентный API;
здесь используется детерминированный эмулятор.
