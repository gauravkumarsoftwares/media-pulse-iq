# ---- Stage 1: Build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY shared-model ./shared-model
COPY insights-query-service ./insights-query-service
RUN mvn -q -pl insights-query-service -am clean package -DskipTests

# ---- Stage 2: Runtime ----
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/insights-query-service/target/*.jar app.jar
EXPOSE 8083
RUN addgroup -S app && adduser -S app -G app
USER app
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-jar", "app.jar"]

