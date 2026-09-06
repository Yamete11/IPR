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
	// GraphQL over HTTP needs the web server + the Spring for GraphQL starter (same as topology-service).
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-graphql")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	// Jackson 3 ObjectMapper — used to parse the incidents JSONB array columns into List<String>.
	implementation("org.springframework.boot:spring-boot-starter-json")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// Boot 4 splits metrics out of the actuator starter: this starter wires Micrometer, the registry
	// exposes /actuator/prometheus.
	implementation("org.springframework.boot:spring-boot-starter-micrometer-metrics")
	runtimeOnly("io.micrometer:micrometer-registry-prometheus")

	runtimeOnly("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	// GraphQlTester slice for the controller test.
	testImplementation("org.springframework.boot:spring-boot-starter-graphql-test")
	// Real TimescaleDB for the repository test (self-skips without Docker). Testcontainers 2.x
	// coordinates, versions from the Boot BOM — see ingestion-service/build.gradle.kts.
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-postgresql")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
