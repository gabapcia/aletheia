run: lint test

ci-deps: ## Install the CI dependencies
	@pip install -Ur requirements.ci.txt

deps: ## Install the APP dependencies
	@pip install -Ur requirements.txt

lint: ## Run lint
	@flake8

test: ## Run tests
	@./pytest.sh --cov

help: ## Display available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
