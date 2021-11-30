FROM openjdk:17-slim
CMD ["/usr/local/openjdk-17/bin/java", "-jar", "/opt/ssdc-rm-job-processor.jar"]

RUN groupadd --gid 999 jobprocessor && \
    useradd --create-home --system --uid 999 --gid jobprocessor jobprocessor

RUN apt-get update && \
apt-get -yq install curl && \
apt-get -yq clean && \
rm -rf /var/lib/apt/lists/*

USER jobprocessor

ARG JAR_FILE=ssdc-rm-job-processor*.jar
COPY target/$JAR_FILE /opt/ssdc-rm-job-processor.jar
