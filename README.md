# Flink Sandbox: Kafka → S3 Parquet

Демонстрационный проект на **Java 21**, **Maven** и **Apache Flink 2.3.0**.

Приложение читает JSON-объекты из Kafka, сохраняет их в S3-совместимое хранилище MinIO в формате Parquet и подсчитывает количество объектов в каждой пачке, определяемой парой `wave_id` и `pack_id`.

Пачка считается завершённой, если для её пары `wave_id`/`pack_id` не поступали новые объекты в течение **5 минут**. Используется processing time Flink: каждое новое событие сбрасывает таймер неактивности.

## Состав проекта

- `WavePackJob` — основная Flink-job.
- `EventGenerator` — генератор JSON-сообщений для Kafka.
- `compose.yaml` — Kafka, MinIO, Flink JobManager и TaskManager.
- `docker/flink/Dockerfile` — образ Flink с собранным приложением.
- `src/test/java` — unit-тесты и тесты с управляемым processing time.

## Требования

- JDK 21.
- Maven 3.9 или новее.
- Docker с Docker Compose.

## Сборка

```bash
mvn clean package
```

После сборки приложение находится в файле:

```text
target/flink-sandbox-1.0.0.jar
```

## Запуск инфраструктуры и Flink-job

```bash
docker compose up --build -d
```

При запуске автоматически:

- создаётся Kafka-топик `input_topic` с тремя партициями;
- создаётся MinIO bucket `flink-data`;
- включается нативный S3-плагин Flink;
- запускается `WavePackJob` в Application Mode.

Доступные интерфейсы:

- Flink Web UI: <http://localhost:8081>
- MinIO Console: <http://localhost:9001>
- MinIO S3 API: `http://localhost:9000`

Данные для входа в MinIO Console:

```text
Логин:  minioadmin
Пароль: minioadmin
```

## Генерация сообщений

После запуска инфраструктуры выполните:

```bash
java -cp target/flink-sandbox-1.0.0.jar \
  com.example.flink.generator.EventGenerator \
  --count 100 \
  --interval-ms 100 \
  --waves 2 \
  --packs 3
```

Команда отправит 100 объектов с интервалом 100 мс. По умолчанию генератор подключается к `localhost:29092` и пишет в `input_topic`.

Параметры генератора:

| Параметр | Значение по умолчанию | Описание |
|---|---:|---|
| `--bootstrap-servers` | `localhost:29092` | Адрес Kafka |
| `--topic` | `input_topic` | Целевой топик |
| `--count` | `100` | Количество сообщений; `0` — непрерывная генерация |
| `--interval-ms` | `100` | Пауза между сообщениями |
| `--waves` | `2` | Количество разных `wave_id` |
| `--packs` | `3` | Количество разных `pack_id` на волну |

Для непрерывной генерации:

```bash
java -cp target/flink-sandbox-1.0.0.jar \
  com.example.flink.generator.EventGenerator \
  --count 0
```

Остановить генератор можно сочетанием клавиш `Ctrl+C`.

Справка по параметрам:

```bash
java -cp target/flink-sandbox-1.0.0.jar \
  com.example.flink.generator.EventGenerator --help
```

## Формат входного JSON

Пример:

```json
{
  "wave_id": "wave-0",
  "pack_id": "pack-0",
  "event_id": "4c8a7e4f-6d81-4a37-938d-d219f8ea6ee2",
  "amount": 123.45,
  "created_at": "2026-11-08T12:00:00Z"
}
```

Поля:

| Поле | Тип | Обязательность |
|---|---|---|
| `wave_id` | string | Обязательное, непустое |
| `pack_id` | string | Обязательное, непустое |
| `event_id` | string или null | Необязательное |
| `amount` | double | Необязательное; при отсутствии используется `0.0` |
| `created_at` | string или null | Необязательное, ISO-8601 в генерируемых сообщениях |

Дополнительные JSON-поля игнорируются. Некорректный JSON или отсутствие обязательных полей приводит к ошибке обработки и остановке job до восстановления по механизму Flink.

## Parquet в MinIO

Файлы сохраняются по путям следующего вида:

```text
s3://flink-data/events/wave_id=wave-0/pack_id=pack-0/events-<id>-<номер>.parquet
```

Parquet-схема содержит пять полей:

```text
wave_id    string
pack_id    string
event_id   nullable string
amount     double
created_at nullable string
```

Файлы становятся доступными для чтения после успешного checkpoint. В конфигурации по умолчанию checkpoints выполняются каждые 10 секунд.

Просмотреть объекты можно через MinIO Console или контейнер с MinIO Client:

```bash
docker compose run --rm --entrypoint /bin/sh minio-init -c '
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc ls --recursive local/flink-data/events
'
```

## Результаты подсчёта пачек

После пяти минут без новых событий для конкретной пары `wave_id`/`pack_id` Flink выводит результат в лог TaskManager:

```bash
docker compose logs -f taskmanager
```

Пример результата:

```text
Completed pack> PackResult{waveId='wave-0', packId='pack-0', processedCount=25, completedAt='...'}
```

Если после завершения придёт событие с той же парой идентификаторов, начнётся новая пачка с отдельным счётчиком.

## Настройка Flink-job

Основные параметры задаются переменными окружения в `compose.yaml`:

| Переменная | Значение по умолчанию | Описание |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` в Compose | Адреса Kafka |
| `KAFKA_TOPIC` | `input_topic` | Входной топик |
| `KAFKA_GROUP_ID` | `flink-wave-pack-counter` | Consumer group |
| `S3_OUTPUT_PATH` | `s3://flink-data/events` | Базовый путь для Parquet |
| `PACK_INACTIVITY_TIMEOUT_MS` | `300000` | Таймаут завершения пачки |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Интервал checkpoints |

При локальном запуске вне Docker `KAFKA_BOOTSTRAP_SERVERS` по умолчанию равен `localhost:29092`.

## Тесты

Запуск всех тестов:

```bash
mvn clean test
```

Тесты покрывают:

- обязательные поля и разбор JSON;
- преобразование в Avro;
- запись и чтение Parquet;
- формирование S3-путей;
- подсчёт пачек, сброс таймера и независимость ключей.

## Остановка и очистка

Остановить контейнеры:

```bash
docker compose down
```

Остановить контейнеры и удалить данные Kafka и MinIO:

```bash
docker compose down -v
```
