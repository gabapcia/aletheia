run: lint test

ci-deps:
	@pip install -Ur requirements.ci.txt

deps:
	@pip install -Ur requirements.txt

lint:
	@flake8
