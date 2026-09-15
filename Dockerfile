# syntax=docker/dockerfile:1
# ============================================================
# Image d'un service OrderFlow - mode AVEC droits administrateur.
# Une seule Dockerfile pour les 4 services, choisi par l'argument SERVICE :
#   docker build --build-arg SERVICE=order-service -t orderflow/order-service .
# En pratique, c'est docker-compose.yml qui la pilote.
# ============================================================

# --- Etape 1 : build Maven du reacteur complet ------------------------------
# Identique pour les 4 images : BuildKit ne l'execute qu'une fois.
# Les tests ne tournent pas ici (mvn verify, cf. README). Les pom.xml ne
# declarent pas spring-boot-maven-plugin : repackage est appele explicitement
# pour obtenir des JAR executables (orderflow-common le saute, skip=true).
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -ntp package spring-boot:repackage -DskipTests

# --- Etape 2 : image d'execution (JRE 25, utilisateur non root) -------------
FROM eclipse-temurin:25-jre
ARG SERVICE
RUN groupadd --system orderflow \
    && useradd --system --gid orderflow --home-dir /app orderflow \
    && mkdir -p /app /data \
    && chown orderflow:orderflow /app /data
WORKDIR /app
COPY --from=build --chown=orderflow:orderflow /workspace/${SERVICE}/target/${SERVICE}-*.jar app.jar
USER orderflow
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
