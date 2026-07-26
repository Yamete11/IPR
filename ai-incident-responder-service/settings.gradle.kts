plugins {
	// Auto-provisions the Java 25 toolchain JDK (this machine's default is JDK 21).
	id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "ai-incident-responder-service"
