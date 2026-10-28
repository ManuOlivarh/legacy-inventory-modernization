# syntax=docker/dockerfile:1

FROM eclipse-temurin:25.0.4_7-jdk-noble AS build
RUN apt-get update \
    && apt-get install --no-install-recommends --yes unzip \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -pl inventory-translator -am package -DskipTests

FROM eclipse-temurin:25.0.4_7-jre-noble
RUN groupadd --system app && useradd --system --gid app app
COPY --from=build /src/inventory-translator/target/inventory-translator.jar /app/app.jar
USER app
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
