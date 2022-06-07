FROM eclipse-temurin:17-jdk-alpine

ARG JAR_FILE=ssdc-rm-job-processor*.jar
CMD ["/opt/java/openjdk/bin/java", "-jar", "/opt/ssdc-rm-job-processor.jar"]
COPY healthcheck.sh /opt/healthcheck.sh
RUN addgroup --gid 1000 jobprocessor && \
    adduser --system --uid 1000 jobprocessor jobprocessor
USER jobprocessor

COPY target/$JAR_FILE /opt/ssdc-rm-job-processor.jar
