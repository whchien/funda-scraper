lint:
	ruff check

format:
	ruff format

test:
	pytest --cov funda_scraper tests/
