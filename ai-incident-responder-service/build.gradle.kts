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
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-json")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// Boot 4 modularised autoconfiguration: the raw `spring-kafka` jar has no Boot autoconfig
	// (no KafkaTemplate/KafkaAdmin/listener-container beans). The starter pulls the `spring-boot-kafka`
	// autoconfig module alongside spring-kafka.
	implementation("org.springframework.boot:spring-boot-starter-kafka")

	// Official Anthropic Java SDK — the Claude API client for the remediation-plan call.
	implementation("com.anthropic:anthropic-java:2.34.0")
	// Resilience4j CORE only (not the Spring-Boot-3 starter, which is not Boot-4 compatible):
	// the CircuitBreaker guarding the Claude call is constructed programmatically as a bean.
	implementation("io.github.resilience4j:resilience4j-circuitbreaker:2.3.0")

	runtimeOnly("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
