# syntax=docker/dockerfile:1
# Build context is the repository root (see compose.yaml), so the Maven wrapper,
# the parent POM and project settings are available to the build stage.

FROM eclipse-temurin:25.0.4_7-jdk-noble AS build
# Without unzip the Maven wrapper falls back to the .tar.gz distribution, whose
# checksum differs from the pinned one in maven-wrapper.properties.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes unzip \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Tests run in CI and `./mvnw verify`; the image build only packages.
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -pl erp-simulator -am package -DskipTests

FROM eclipse-temurin:25.0.4_7-jre-noble
RUN groupadd --system app && useradd --system --gid app app
COPY --from=build /src/erp-simulator/target/erp-simulator.jar /app/app.jar
USER app
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
