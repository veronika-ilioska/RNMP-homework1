# syntax=docker/dockerfile:1
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp package

FROM flink:2.3.0-java17
RUN mkdir -p /opt/flink/savepoints && chown flink:flink /opt/flink/savepoints
COPY --from=build --chown=flink:flink /build/target/github-pipeline.jar /opt/flink/usrlib/github-pipeline.jar
