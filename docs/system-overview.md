# Обзор системы: архитектура, флоу, Kafka, гарантии доставки

Состояние на 2026-10-25, после исправления багов (см. раздел «Bug sweep» в [`../CLAUDE.md`](../CLAUDE.md)).
Контракты сервисов и схемы сообщений: [`microservices.md`](microservices.md). Диаграммы:
[`architecture.mmd`](architecture.mmd), [`incident-response-sequence.mmd`](incident-response-sequence.mmd).

## Компоненты

```
sensor-simulator (Python) ──► Kafka: city-telemetry ──► ingestion-service ──► TimescaleDB.metrics
                                                                                     │
                                       anomaly-detector-service  ◄── опрос раз в 15 с ┘ (CA metrics_5m_avg)
                                                │
                                                ▼
                                   Kafka: infrastructure-alerts
                                                │
                                                ▼
                           ai-incident-responder-service ──► Claude API
                             читает TimescaleDB (история) + age-db (граф)
                                                │
                                                ▼
                                    TimescaleDB.incidents
                                                │
        dashboard (Angular/nginx) ──► incident-api-service (GraphQL :8081) ── incidents + metrics
                                  └─► topology-service   (GraphQL :8080) ── граф в age-db
```

Сервисы не вызывают друг друга напрямую. Конвейер общается через Kafka, а общие данные лежат в двух
Postgres:

- **`timescaledb`**: метрики, continuous aggregate, инциденты.
- **`age-db`**: граф сети. Писать в него может только `topology-service`.

## Флоу шаг за шагом

1. **Симулятор** каждые N секунд отправляет по одному показанию с каждого датчика:
   `{timestamp, sensor_id, metric_type, value, status}` в `city-telemetry`.
   - Ключ сообщения — `sensor_id`, поэтому все показания одного датчика попадают в одну партицию и
     идут по порядку.
   - Значения — случайное блуждание с возвратом к норме и случайными сбоями: напряжение падает в 0,
     давление в 0, температура растёт до 130.
2. **ingestion-service** — batch-листенер, за один poll получает до 500 записей.
   - Каждая запись парсится и валидируется: нет обязательных полей, `NaN`/`Infinity`, timestamp
     дальше 5 минут в будущем → битая (poison) запись.
   - Записи до битой вставляются одним JDBC-батчем:
     `INSERT ... ON CONFLICT (timestamp, sensor_id, metric_type) DO NOTHING`.
   - Затем бросается `BatchListenerFailedException(index)`. Spring коммитит offset'ы до неё, битую
     запись отправляет в `city-telemetry.DLQ`, а хвост батча перечитывает.
3. **TimescaleDB:** таблица `metrics` — hypertable с интервалом 1 день, сжатием через 7 дней и
   удалением через 30 дней.
   - `anomaly-detector` при старте создаёт continuous aggregate `metrics_5m_avg`: avg/min/max за
     каждые 5 минут.
   - `materialized_only=false`: свежие строки, которые ещё не попали в агрегат, всё равно видны в
     запросах.
4. **anomaly-detector** раз в 15 секунд берёт два последних 5-минутных интервала по каждой серии
   (датчик + метрика) за последние 15 минут.
   - Применяет правила из `anomaly.rules[]`: `pipe_pressure min < 0.5`, `cable_temperature max > 120`,
     `substation_voltage min < 50`.
   - Алерт отправляется только на переходе «норма → сбой» (edge-trigger). Пока сбой держится,
     повторных алертов нет.
   - Серия снова «вооружается» после восстановления (оба интервала в норме) или если перестала
     присылать данные.
   - Алерт `{alert_id (UUID), sensor_id, metric_type, value, detected_at, rule}` уходит в
     `infrastructure-alerts` с ключом `sensor_id`.
5. **ai-incident-responder** на каждый алерт делает следующее:
   - Парсит алерт. Битый сразу уходит в `infrastructure-alerts.DLQ`.
   - Проверяет, нет ли уже инцидента по этому `alert_id`. Повторная доставка пропускается до вызова
     Claude, за который платят.
   - Собирает контекст (лёгкий RAG):
     - сводку метрики за час до `detected_at` из TimescaleDB;
     - из age-db: объект, за которым следит датчик (`SensorLink`); всё, что от него зависит, вместе с
       ответственными бригадами — один гибридный запрос `cypher(... [*0..3] ...) LEFT JOIN
       public.crew_assignments / repair_crews`; и объекты выше по графу (`<-[*1..3]-`) для поиска
       первопричины.
   - **Ищет первопричину** (см. раздел «Корреляция инцидентов» ниже). Если выше по графу (или на том же
     объекте) уже есть активный инцидент, алерт становится его дочерним инцидентом **без вызова Claude**.
   - Иначе вызывает Claude: `claude-opus-5-5`, adaptive thinking с effort `high`, structured output в
     `IncidentPlan` (criticality, impacted objects, remediation steps, summary). В промпте есть раздел
     `## Responsible crews`. Вызов идёт через Resilience4j circuit breaker.
   - Пишет результат в `incidents` с `ON CONFLICT (alert_id) DO NOTHING` — в одной транзакции под
     advisory-lock с повторной проверкой корня и «усыновлением» уже открытых инцидентов ниже по графу.
   - При `AI_CLAUDE_MOCK=true` вместо Claude работает детерминированный `MockIncidentPlanner`.
6. **incident-api / topology-service** отдают дашборду данные через GraphQL, только на чтение. В
   браузере всё идёт через один origin: `/api/incidents/graphql` и `/api/topology/graphql`,
   проксируются через nginx (или `proxy.conf.json` в dev).

## Граф города и корреляция инцидентов

### Граф

- `topology-service` при старте загружает в age-db ручной сид (`seed/city-topology.json`: `sub-1`,
  `node-5`, …, датчики `s-00042`/`s-00108`/`s-00311`) и **сгенерированный город** для синтетических
  датчиков симулятора (`SyntheticCity`, `topology.synthetic.*`).
- Правило раскладки одно на Java и Python (`SyntheticCity` / `sensor_simulator.py:layout_of`):
  датчик `s-{1000+i}` входит в тройку `t = i/3`; тройка владеет домом `t`; дом висит на узле `t/4`,
  узел — на подстанции `узел/4`. Датчик давления следит за домом, кабеля — за узлом, напряжения — за
  подстанцией.
- Id: `sub-dDD`, `node-dDD-k`, `bld-dDD-k-b`; у каждой сгенерированной вершины свойство
  `district`; на каждый район бригада `crew-dDD`.
- Число датчиков задаёт одна переменная `CITY_EXTRA_SENSORS` (ConfigMap, 997): по ней генерирует
  город topology-service, и её же симулятор берёт как `--extra-sensors` по умолчанию. При 997 — 21
  подстанция, 84 узла, 333 дома, 1444 вершины и 1422 ребра вместе с ручным сидом.
- Загрузка идёт одной транзакцией пачками `UNWIND ... CREATE`. Отпечаток раскладки хранится в
  `public.topology_seed_state`, поэтому перезапуск ничего не перезаписывает, а смена числа датчиков
  перезагружает город.

### Корреляция инцидентов (первопричина)

Responder смотрит, нет ли **активного корневого инцидента** на объекте алерта или выше него по графу
(`IncidentCorrelator`, `ai.correlation.enabled`, `ai.correlation.window`, по умолчанию 30 минут).

- **Активный** = `detected_at` в пределах ±окна от `detected_at` нового алерта. Окно в обе стороны,
  потому что детектор публикует алерты одного опроса в порядке `sensor_id`, и алерт дома часто
  приходит раньше алерта своей подстанции. Сигналов восстановления детектор не шлёт, так что время —
  единственный признак.
- **Нашёлся корень** → дочерний инцидент: `root_incident_id` = корень, критичность наследуется,
  `model = correlated`, **Claude не вызывается**. Из нескольких кандидатов выигрывает самый верхний
  объект.
- **Корня нет** → обычный план. После вставки новый корень **усыновляет** корневые инциденты, уже
  открытые ниже по графу в том же окне, вместе с их детьми. Группа всегда глубиной 1.
- Повторная проверка, вставка и усыновление идут в одной транзакции под
  `pg_advisory_xact_lock`, поэтому две реплики не создадут два корня одной аварии. Под блокировкой
  только быстрый SQL, вызов Claude — снаружи.
- Новые колонки `incidents`: `asset_id`, `root_incident_id`, `detected_at`, `crews` (JSONB). Их
  добавляет `schema.sql` responder через `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. incident-api
  отдаёт их, плюс поля `rootIncident` и `linkedIncidents`.

**Это эвристика, а не доказательство причины:** совпадение по времени на пути зависимостей. При
случайных сбоях (`--anomaly-prob 0.02` на 1000 датчиков почти каждый датчик падает за секунды)
корреляция свяжет независимые сбои. Поэтому профиль симулятора в чарте — каскады
(`--anomaly-prob 0 --cascade-every 300`): раз в 5 минут у следующего района пропадает напряжение на
подстанции, а через 5 секунд падает давление во всех его домах. Итог — 1 план и до 31 привязанного
инцидента (16 датчиков давления и ещё 15 датчиков напряжения той же подстанции).

## Kafka

### Брокер

- В compose: `apache/kafka:3.9.1`, одна нода в режиме KRaft (broker и controller в одном процессе).
- В K8s: Strimzi 1.x, `Kafka` + `KafkaNodePool`, KRaft, одна нода с ролями controller и broker,
  Kafka 4.3.1 ([`../deploy/strimzi/kafka.yaml`](../deploy/strimzi/kafka.yaml)).
- `replication.factor=1`, `min.insync.replicas=1`, `auto.create.topics.enable=false`. Топики
  создаются как `KafkaTopic` CR в [`../deploy/strimzi/topics.yaml`](../deploy/strimzi/topics.yaml).

### Топики (K8s)

| Топик | Партиции | Кто пишет | Кто читает |
|---|---|---|---|
| `city-telemetry` | 6 (retention 1 день) | simulator | ingestion-service |
| `city-telemetry.DLQ` | 6 | ingestion (recoverer) | никто (для разбора вручную) |
| `infrastructure-alerts` | 3 | anomaly-detector | ai-incident-responder |
| `infrastructure-alerts.DLQ` | 3 | responder (recoverer) | никто |

В compose топики создают бины `NewTopic` в сервисах, по 3 партиции.

### Продюсеры

| Продюсер | Настройки |
|---|---|
| simulator (confluent-kafka) | `enable.idempotence=true` (а значит `acks=all`), `linger.ms=50`, `compression.type=lz4`, ключ `sensor_id`, delivery callback логирует ошибки |
| anomaly-detector | Java `KafkaTemplate` со `StringSerializer`; acks/idempotence явно не заданы, действуют дефолты клиента Kafka 3+ (`acks=all`, `enable.idempotence=true`). При ошибке доставки ключ серии снимается, и следующий опрос отправляет алерт заново |
| DLQ-продюсеры ingestion и responder | отдельный `KafkaTemplate`, партицию выбирает продюсер (`-1`) |

### Консьюмеры

| | ingestion-service | ai-incident-responder |
|---|---|---|
| group-id | `ingestion-service` | `ai-incident-responder` |
| тип листенера | batch (`max-poll-records: 500`, `fetch-min-size: 64KB`, `fetch.max.wait.ms: 500`) | по одной записи |
| `enable-auto-commit` | `false` (коммитит контейнер Spring после обработки) | `false` |
| `auto-offset-reset` | `earliest` | `earliest` |
| assignor | `CooperativeStickyAssignor` | `CooperativeStickyAssignor` |
| масштабирование | KEDA `ScaledObject` по лагу группы, 1→6 подов (максимум = число партиций) | 1 под |

`CooperativeStickyAssignor` выбран потому, что дефолтный eager `range` при каждом шаге KEDA
останавливал все реплики на ребалансе. Cooperative перераспределяет только те партиции, которые
переезжают.

### Обработка ошибок (`DefaultErrorHandler`)

- Неретраимые ошибки сразу уходят в DLQ:
  - ingestion: `InvalidTelemetryException`, `DataIntegrityViolationException`;
  - responder: `InvalidAlertException`, `UnplannableAlertException` (refusal / `max_tokens` / нет
    контента), Anthropic 400/401/403/404/422 (`ResilienceConfig.PERMANENT_FAILURES`).
- Всё остальное повторяется бесконечно с экспоненциальной паузой от 1 до 30 секунд: БД недоступна,
  Claude 429/5xx, ошибки сети, открытый breaker. Ценные данные не теряются в DLQ, который никто не
  читает.
- Пауза реализована через `ContainerPausingBackOffHandler`: контейнер ставится на паузу, а не спит в
  потоке консьюмера. Иначе долгий сбой превысил бы `max.poll.interval.ms` и вызвал бесконечный
  ребаланс.
- Circuit breaker: окно 5 вызовов, открывается после минимум 3 вызовов при 50% ошибок. Открыт 30
  секунд, в полуоткрытом состоянии пропускает 2 пробных вызова. Пока открыт, вызовы получают
  `CallNotPermittedException`, и алерт ждёт, а не уходит в DLQ. Постоянные ошибки
  (`PERMANENT_FAILURES`) breaker игнорирует — они не говорят о доступности API.

## Гарантии доставки

В целом это **at-least-once плюс идемпотентные потребители**, что на практике даёт effectively-once
на уровне базы. Kafka-транзакций и EOS (exactly-once) нет.

| Участок | Гарантия | Почему |
|---|---|---|
| simulator → Kafka | без дублей на брокере при ретраях | идемпотентный продюсер, `acks=all` (при RF=1 это фактически подтверждение одного брокера) |
| Kafka → TimescaleDB | at-least-once, дубли схлопываются | offset коммитится после записи в БД; при повторной доставке срабатывает `ON CONFLICT DO NOTHING` по `(timestamp, sensor_id, metric_type)` |
| detector → Kafka | at-least-once на уровне «один алерт на сбой» | ключ серии снимается при ошибке отправки, следующий опрос шлёт заново; дубль возможен, если ack потерян после записи |
| Kafka → incidents | at-least-once, дубли схлопываются | проверка `existsByAlertId` до Claude плюс `UNIQUE(alert_id)` с `ON CONFLICT`; при гонке план выбрасывается, но в БД одна строка |
| битые сообщения | изолируются | DLQ, партиция не блокируется |

### Где гарантии слабее

- **RF=1:** если упадёт единственный брокер вместе с диском, данные потеряются. Для локального
  стенда это осознанный компромисс.
- **Состояние дедупликации детектора хранится в памяти:** после рестарта детектора активные сбои
  дадут новый алерт с новым `alert_id`. Если датчик есть в графе и прошлый инцидент на его объекте
  ещё в окне корреляции, новый алерт станет дочерним инцидентом без вызова Claude; иначе responder
  создаст ещё один инцидент.
- **Повторная доставка в responder** может вызвать Claude дважды и выбросить второй план. Это лишние
  деньги, но не потеря данных.
- **DLQ никто не читает:** разбор только вручную.

## Подробнее

- Kafka с реальным выводом брокера: [`kafka.md`](kafka.md)
- Хранение временных рядов: [`timeseries-storage.md`](timeseries-storage.md)
- Граф против SQL: [`graph-vs-sql.md`](graph-vs-sql.md)
- K8s, хранилище и масштабирование: [`k8s-storage-and-scaling.md`](k8s-storage-and-scaling.md)
- LLM-интеграция: [`llm-integration.md`](llm-integration.md)
