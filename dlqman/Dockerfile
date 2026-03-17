FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src

# Clone DLQman source
RUN git clone --depth 1 --branch v0.5.0 https://github.com/irori-ab/dlqman.git .

# Replace application.yaml with our custom config
COPY application.yaml src/main/resources/application.yaml

# Add a REST endpoint for listing messages (not included in v0.5.0)
COPY MessageResource.java src/main/java/se/irori/rest/MessageResource.java

# Add a static HTML dashboard so the root path serves a UI
COPY index.html src/main/resources/META-INF/resources/index.html

# Build the Quarkus application (skip tests)
RUN mvn package -DskipTests -B -Dquarkus.package.type=fast-jar

FROM eclipse-temurin:17-jre
WORKDIR /deployments

COPY --from=build /src/target/quarkus-app/ ./

EXPOSE 8000

ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
