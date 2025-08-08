install:
	mvn clean install

build: install docker-build

build-no-test: install-no-test docker-build

install-no-test:
	mvn clean install -Dmaven.test.skip=true -Dexec.skip=true -Djacoco.skip=true

format:
	mvn fmt:format

format-check:
	mvn fmt:check

check:
	mvn fmt:check pmd:check

test:
	mvn clean verify jacoco:report

docker-build:
	docker build . -t europe-west2-docker.pkg.dev/ssdc-rm-ci/docker/ssdc-rm-job-processor:latest

megalint:  ## Run the mega-linter.
	docker run --platform linux/amd64 --rm \
		-v /var/run/docker.sock:/var/run/docker.sock:rw \
		-v $(shell pwd):/tmp/lint:rw \
		oxsecurity/megalinter:v8

megalint-fix:  ## Run the mega-linter and attempt to auto fix any issues.
	docker run --platform linux/amd64 --rm \
		-v /var/run/docker.sock:/var/run/docker.sock:rw \
		-v $(shell pwd):/tmp/lint:rw \
		-e APPLY_FIXES=all \
		oxsecurity/megalinter:v8

clean_megalint: ## Clean the temporary files.
	rm -rf megalinter-reports

lint_check: clean_megalint megalint
