-- First-boot only: create the AGE extension in the age-db database.
-- The graph itself (create_graph) is created by topology-service's seed loader,
-- not here. Bind-mounted into /docker-entrypoint-initdb.d by docker-compose.yml.
CREATE EXTENSION IF NOT EXISTS age;
