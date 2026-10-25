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
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-graphql")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// Boot 4 splits metrics out of the actuator starter: this starter wires Micrometer, the registry
	// exposes /actuator/prometheus.
	implementation("org.springframework.boot:spring-boot-starter-micrometer-metrics")
	runtimeOnly("io.micrometer:micrometer-registry-prometheus")

	runtimeOnly("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.graphql:spring-graphql-test")
	// @GraphQlTest slice for the controller test.
	testImplementation("org.springframework.boot:spring-boot-starter-graphql-test")
	// Real Apache AGE for the repository + benchmark tests (self-skip without Docker). Testcontainers
	// 2.x coordinates, versions from the Boot BOM — see ingestion-service/build.gradle.kts.
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-postgresql")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

}

tasks.withType<Test> {
	useJUnitPlatform()
}

// The graph-vs-SQL benchmark builds a ~10k-vertex graph and times queries: keep it out of `build`.
tasks.test {
	useJUnitPlatform { excludeTags("benchmark") }
}

val graphBenchmark by tasks.registering(Test::class) {
	description = "Cypher variable-length traversal vs. recursive CTE on a synthetic topology (needs Docker)."
	group = "verification"
	testClassesDirs = sourceSets.test.get().output.classesDirs
	classpath = sourceSets.test.get().runtimeClasspath
	useJUnitPlatform { includeTags("benchmark") }
	outputs.upToDateWhen { false }
	testLogging { showStandardStreams = true }
}
