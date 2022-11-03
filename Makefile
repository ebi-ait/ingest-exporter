install:
	pip install pip-tools
	pip-sync requirements.txt dev-requirements.txt

test:
	python -m pytest tests
