install:
	pip install --upgrade pip && pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:
	black *.py 

lint:
	ruff check --line-length 320 *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	# Deploy goes here

all: install lint test format deploy

run:
	python main.py

job:
	python run_job.py
