plugins {
	java
	id("org.springframework.boot") version "4.1.0"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "inno.ipr"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter")
	// The service has no HTTP API of its own; the web starter exists so the actuator can serve
	// /actuator/health/{liveness,readiness} to Kubernetes probes and /actuator/prometheus to a scraper.
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-json")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// Boot 4 splits metrics out of the actuator starter: this starter wires Micrometer (including the
	// Kafka consumer metrics spring-kafka registers), the registry exposes them in Prometheus format.
	implementation("org.springframework.boot:spring-boot-starter-micrometer-metrics")
	runtimeOnly("io.micrometer:micrometer-registry-prometheus")
	// Boot 4 modularised autoconfiguration: the raw `spring-kafka` jar has no Boot autoconfig
	// (no KafkaTemplate/KafkaAdmin/listener-container beans). The starter pulls the `spring-boot-kafka`
	// autoconfig module alongside spring-kafka.
	implementation("org.springframework.boot:spring-boot-starter-kafka")

	runtimeOnly("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	// In-JVM broker for the error-handler/DLQ integration test — no Docker needed.
	testImplementation("org.springframework.kafka:spring-kafka-test")
	// Real TimescaleDB for the schema-initialisation test. Those tests self-skip when Docker is
	// unavailable, so `./gradlew build` still passes on a machine without it.
	// Testcontainers 2.x artifact names (`testcontainers-*`); versions come from the Boot 4.1 BOM.
	// The legacy `org.testcontainers:junit-jupiter`/`:postgresql` coordinates are unmanaged here and
	// drag the 1.x modules against a 2.x core.
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-postgresql")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
