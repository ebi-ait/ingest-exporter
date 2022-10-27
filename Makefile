install:
	pip install pip-tools
	pip-compile dev-requirements.in
	pip-compile requirements.in
	pip-sync requirements.txt dev-requirements.txt

test:
	pytest tests