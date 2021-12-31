build:
	mvn clean install

build-no-test:
	mvn clean install -Dmaven.test.skip=true -DdockerCompose.skip=true

format:
	mvn fmt:format

format-check:
	mvn fmt:check

test:
	mvn clean verify jacoco:report

docker-build:
	docker build . -t eu.gcr.io/ssdc-rm-ci/rm/ssdc-rm-case-api:latest
