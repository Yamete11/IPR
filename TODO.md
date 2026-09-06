# TODO — topology-service: read layer (GraphQL over the AGE graph)

## Контекст

`topology-service` (Java 25 / Spring Boot 4.1.0) уже владеет **write**-стороной графа Apache AGE:
`age/AgeClient.java` + `seed/SeedLoader.java` идемпотентно засеивают `city-topology.json`
(9 вершин, 8 рёбер) под защитой allow-list'ов `VertexLabel`/`EdgeType`. Это готово и проверено
на реальном `age-db`.

Не хватает **read-API** для дашборда. Read-путь наполовину написан и **недоступен**:

- `resources/graphql/schema.graphqls` — всё ещё заглушка `ping: String` (нет типов `Vertex`/`Edge`,
  нет query-полей).
- `graphql/TopologyGraphQLController.java` — есть методы `vertices()`/`edges()`, но **без
  `@QueryMapping`**, поэтому Spring for GraphQL их не регистрирует.
- `service/TopologyQueryService.java` — read-Cypher работает, но дефекты: `jdbc` объявлен как
  `private static` (анти-паттерн), а `properties` отдаётся **сырой agtype-строкой**, а не JSON.
- `repository/` — пустой `.gitkeep`; read-Cypher сейчас лежит прямо в сервисе.

**Цель:** рабочий GraphQL read-API — полный граф (`vertices` + `edges`) для сетевого вида **плюс
обходы зависимостей** (`dependents`/`dependencies`) — ключевая фича из design-доков («найти все
здания, зависящие от Distribution Node #5»). Вершины моделируем обобщённо
(`Vertex { id, label, properties }`), `properties` — реальный JSON-скаляр. Проверяем **запуском**
приложения на `age-db`, не тестами.

**Решения (согласовано):** (1) объём = полный граф **+** обходы зависимостей (мутаций на этом шаге
нет — и по итогам аудита 2026-09-06 решено оставить API **read-only**: единственный путь записи в
граф — seed-файл при старте; это принятое отклонение от фразы «asset/topology management» в
`docs/microservices.md`); (2) модель вершин = один обобщённый тип `Vertex` с `properties` JSON-скаляром (парсим
agtype → JSON; типизированной per-label схемы нет).

## Шаги

- [x] **1. JSON-скаляр (зависимость).** `topology-service/build.gradle.kts` — добавить
  `implementation("com.graphql-java:graphql-java-extended-scalars:<совместимая версия>")`
  (совместимо с graphql-java из BOM Boot 4.1.0), чтобы получить `ExtendedScalars.Json`.

- [x] **2. Парсер agtype → Java.** Новый `age/AgtypeParser.java` (или статические методы в
  `AgeClient`):
  - `parseScalar(String)` — снять кавычки, которыми AGE оборачивает строковый agtype (замена
    ad-hoc `cleanAgtype` из сервиса);
  - `parseMap(String) -> Map<String,Object>` — распарсить agtype-map в `Map` через Jackson 3
    (`tools.jackson.databind.ObjectMapper`); для `null`/`{}` вернуть пустую map.

- [x] **3. Repository (вынести Cypher из сервиса, убрать static).** Новый
  `repository/TopologyRepository.java` — `@Repository`, **инстансный** `JdbcTemplate` (через
  конструктор — убивает static-дефект). Весь read-Cypher как
  `SELECT * FROM cypher('city_topology', $$ ... $$) AS (...)`, маппинг в DTO через `AgtypeParser`:
  - `findAllVertices()` → `MATCH (n) RETURN n.id, label(n), properties(n)`;
  - `findAllEdges()` → `MATCH (a)-[r]->(b) RETURN type(r), a.id, b.id, properties(r)`;
  - `findDependents(String id)` — вниз по цепочке (кто теряет сервис при отказе `id`):
    `MATCH (n {id:$id})-[:PowerLine|Pipeline*1..]->(d) RETURN DISTINCT d.id, label(d), properties(d)`;
  - `findDependencies(String id)` — вверх (от чего зависит `id`):
    `MATCH (n {id:$id})<-[:PowerLine|Pipeline*1..]-(u) RETURN DISTINCT ...`;
  - аргумент `id` биндить как agtype-параметр (`?::agtype`, `Types.OTHER`) как в `AgeClient.execute`,
    **не** интерполировать строкой (защита от инъекции). Проверить при реализации, что AGE 1.6.0
    принимает `|`-объединение типов внутри var-length; если нет — откат на `-[*1..]->` + фильтр.

- [x] **4. DTO: properties как JSON, не String.**
  - `graphql/dto/VertexDto.java` — `properties`: `String` → `Map<String,Object>`;
  - `graphql/dto/EdgeDto.java` — то же.

- [x] **5. Тонкий сервис.** `service/TopologyQueryService.java` — убрать static `jdbc`, инлайновый
  SQL и `cleanAgtype`; делегировать в `TopologyRepository`. Оставить как слой, который зовёт
  контроллер (`findAllVertices`, `findAllEdges`, `findDependents(id)`, `findDependencies(id)`).

- [x] **6. Реальная GraphQL-схема.** Заменить `resources/graphql/schema.graphqls`:
  ```graphql
  scalar JSON

  type Vertex { id: ID!, label: String!, properties: JSON }
  type Edge   { type: String!, from: ID!, to: ID!, properties: JSON }

  type Query {
    vertices: [Vertex!]!
    edges: [Edge!]!
    dependents(id: ID!): [Vertex!]!
    dependencies(id: ID!): [Vertex!]!
  }
  ```

- [x] **7. Регистрация JSON-скаляра.** Новый `config/GraphQlConfig.java` — бин
  `RuntimeWiringConfigurer`, подключающий `ExtendedScalars.Json` под скаляр `JSON`.

- [x] **8. Подключить контроллер.** `graphql/TopologyGraphQLController.java` — навесить
  `@QueryMapping` на четыре метода; добавить `@Argument String id` в `dependents`/`dependencies`.
  Имена методов = имена полей схемы.

- [x] **9. Мелочь (опц., низкий риск).** `seed/model/SeedGraph.java` импортирует Jackson **2**
  `@JsonIgnoreProperties`, тогда как остальной код на Jackson 3. Оставить или убрать — не блокирует.

## Критические файлы

- `topology-service/build.gradle.kts` — dep extended-scalars.
- `topology-service/src/main/resources/graphql/schema.graphqls` — реальная схема.
- `topology-service/src/main/java/inno/ipr/topology_service/`
  - `age/AgtypeParser.java` (новый)
  - `repository/TopologyRepository.java` (новый — заполняет пустой stub)
  - `config/GraphQlConfig.java` (новый — заполняет пустой stub)
  - `service/TopologyQueryService.java` (переписать: делегировать, убрать static)
  - `graphql/TopologyGraphQLController.java` (`@QueryMapping`/`@Argument`)
  - `graphql/dto/VertexDto.java`, `graphql/dto/EdgeDto.java` (properties → `Map<String,Object>`)

Переиспользовать (не изобретать заново): идиому `?::agtype` + `Types.OTHER` из `AgeClient.execute`,
allow-list'ы `VertexLabel`/`EdgeType`, `connection-init-sql` (search_path) из `application.yaml`
(без per-query `LOAD 'age'`).

## Проверка (запуском, не тестами)

1. Поднять `age-db`: `docker compose up -d age-db` (из корня — двух-БД compose уже есть).
2. Собрать/запустить: `cd topology-service && ./gradlew bootRun`
   (`topology.seed.enabled: true` засеет граф; SeedLoader логирует 9 вершин / 8 рёбер).
3. Открыть **GraphiQL** на `http://localhost:8080/graphiql` (уже включён в `application.yaml`) и
   выполнить:
   - `{ vertices { id label properties } }` — ожидаем 9, `properties` как JSON-объект
     (напр. sub-1 → `{ "name": "Central Substation", "voltage_kv": 110 }`), не строку;
   - `{ edges { type from to properties } }` — ожидаем 8;
   - `{ dependents(id: "node-5") { id label } }` — здания, питаемые node-5 (`bld-12`, `bld-19`);
   - `{ dependencies(id: "bld-12") { id label } }` — цепочка вверх (`node-5`, `sub-1`).
4. Повторный запуск сохраняет 9/8 (идемпотентность seed); неверный id возвращает `[]` без ошибок.
