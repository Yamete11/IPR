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
	// No HTTP API of its own; the web starter exists so the actuator can serve
	// /actuator/health/{liveness,readiness} to Kubernetes probes and /actuator/prometheus to a scraper.
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// Boot 4 splits metrics out of the actuator starter: this starter wires Micrometer, the registry
	// exposes /actuator/prometheus.
	implementation("org.springframework.boot:spring-boot-starter-micrometer-metrics")
	runtimeOnly("io.micrometer:micrometer-registry-prometheus")
	// Boot 4 modularised autoconfiguration: the raw `spring-kafka` jar has no Boot autoconfig
	// (no KafkaTemplate/KafkaAdmin/listener-container beans). The starter pulls the `spring-boot-kafka`
	// autoconfig module alongside spring-kafka.
	implementation("org.springframework.boot:spring-boot-starter-kafka")

	// Official Anthropic Java SDK — the Claude API client for the remediation-plan call.
	implementation("com.anthropic:anthropic-java:2.34.0")
	// Resilience4j CORE only (not the Spring-Boot-3 starter, which is not Boot-4 compatible):
	// the CircuitBreaker guarding the Claude call is constructed programmatically as a bean.
	implementation("io.github.resilience4j:resilience4j-circuitbreaker:2.3.0")
	// Publishes the breaker's state/calls/failure-rate as Micrometer meters (resilience4j_circuitbreaker_*).
	implementation("io.github.resilience4j:resilience4j-micrometer:2.3.0")

	runtimeOnly("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	// In-JVM broker for the alert-pipeline integration test — no Docker needed.
	testImplementation("org.springframework.kafka:spring-kafka-test")
	// Real Apache AGE + TimescaleDB for the graph/correlation integration tests (self-skip without
	// Docker). Testcontainers 2.x coordinates, versions from the Boot BOM — as in topology-service.
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-postgresql")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}

// The token-budget test calls the Claude count-tokens endpoint (needs ANTHROPIC_API_KEY): keep it out of `build`.
tasks.test {
	useJUnitPlatform { excludeTags("llm") }
}

val promptTokens by tasks.registering(Test::class) {
	description = "Counts the incident prompt's input tokens with the Claude count-tokens endpoint (needs ANTHROPIC_API_KEY)."
	group = "verification"
	testClassesDirs = sourceSets.test.get().output.classesDirs
	classpath = sourceSets.test.get().runtimeClasspath
	useJUnitPlatform { includeTags("llm") }
	outputs.upToDateWhen { false }
	testLogging { showStandardStreams = true }
}
