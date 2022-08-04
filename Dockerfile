FROM eclipse-temurin:17-jre-alpine

CMD ["java", "-jar", "/opt/ssdc-rm-job-processor.jar"]

COPY healthcheck.sh /opt/healthcheck.sh
RUN addgroup --gid 1000 jobprocessor && \
    adduser --system --uid 1000 jobprocessor jobprocessor
USER jobprocessor

COPY target/ssdc-rm-job-processor*.jar /opt/ssdc-rm-job-processor.jar
