# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace/backend

ARG MODULE
RUN test -n "$MODULE"

COPY backend/mvnw backend/mvnw.cmd backend/pom.xml ./
COPY backend/.mvn ./.mvn
COPY backend/event-contracts ./event-contracts
COPY backend/order-service ./order-service
COPY backend/inventory-service ./inventory-service
COPY backend/kitchen-service ./kitchen-service
COPY backend/order-query-service ./order-query-service

RUN --mount=type=cache,target=/root/.m2 \
    JAR_PATH=$(find "${MODULE}/target" -maxdepth 1 -type f -name '*.jar' ! -name '*.original' 2>/dev/null | head -n 1 || true); \
    if [ -z "${JAR_PATH}" ]; then \
        chmod +x ./mvnw && ./mvnw -B -ntp -pl "${MODULE}" -am package -DskipTests; \
        JAR_PATH=$(find "${MODULE}/target" -maxdepth 1 -type f -name '*.jar' ! -name '*.original' | head -n 1); \
    fi; \
    test -n "${JAR_PATH}" && cp "${JAR_PATH}" /app.jar

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN command -v curl >/dev/null 2>&1 || (apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*)
COPY --from=build /app.jar /app/app.jar
ENTRYPOINT ["java", "-Duser.timezone=UTC", "-jar", "/app/app.jar"]
