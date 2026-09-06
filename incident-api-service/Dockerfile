# syntax=docker/dockerfile:1.7
# Multi-stage build for incident-api-service. The committed Gradle wrapper runs inside a JDK 25 image, so the
# foojay toolchain resolver finds the build JVM already satisfies `languageVersion = 25` and
# downloads nothing. The runtime stage is the JRE only, with the Boot jar exploded into layers
# (dependencies change rarely, application code often) so image rebuilds are small.
#
#   docker build -t smartcity/incident-api-service:dev .
#
# Alternative with no Dockerfile at all: `./gradlew bootBuildImage` (Paketo buildpacks).

FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace

# Wrapper + build scripts first so the dependency download is cached until they change.
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN chmod +x gradlew
RUN --mount=type=cache,target=/root/.gradle,id=gradle-incident-api-service \
    ./gradlew --no-daemon dependencies --quiet > /dev/null

COPY src ./src
RUN --mount=type=cache,target=/root/.gradle,id=gradle-incident-api-service \
    ./gradlew --no-daemon bootJar -x test --quiet \
 && cp build/libs/*-SNAPSHOT.jar application.jar \
 && java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:25-jre
WORKDIR /application
# Numeric non-root user (the base image already owns UID 1000); matches runAsUser in the charts.
COPY --from=build --chown=1000:1000 /workspace/extracted/dependencies/ ./
COPY --from=build --chown=1000:1000 /workspace/extracted/spring-boot-loader/ ./
COPY --from=build --chown=1000:1000 /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build --chown=1000:1000 /workspace/extracted/application/ ./
USER 1000:1000
# Size the heap from the container's memory limit instead of the node's RAM.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "application.jar"]
