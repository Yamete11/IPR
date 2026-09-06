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
	// No HTTP API of its own; the web starter exists so the actuator can serve
	// /actuator/health/{liveness,readiness} to Kubernetes probes and /actuator/prometheus to a scraper.
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-json")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// Boot 4 splits metrics out of the actuator starter: this starter wires Micrometer, the registry
	// exposes /actuator/prometheus.
	implementation("org.springframework.boot:spring-boot-starter-micrometer-metrics")
	runtimeOnly("io.micrometer:micrometer-registry-prometheus")
	// Boot 4 modularised autoconfiguration: the raw `spring-kafka` jar has no Boot autoconfig
	// (no KafkaTemplate/KafkaAdmin beans). The starter pulls the `spring-boot-kafka` autoconfig
	// module alongside spring-kafka. This service is a producer only (no @KafkaListener, no DLQ).
	implementation("org.springframework.boot:spring-boot-starter-kafka")

	runtimeOnly("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	// In-JVM broker for the detection-pipeline integration test — no Docker needed.
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
