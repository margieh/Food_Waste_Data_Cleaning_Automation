.PHONY: run clean test

VENV = . venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip3
PIPELINE_DIR = src

setup: 
	python3 -m venv venv
	. venv/bin/activate
	pip3 install -r requirements.txt

run_merge: setup
	cd $(PIPELINE_DIR) && python3 pipeline.py -subdir raw -merge True

run: setup
	cd $(PIPELINE_DIR) && python3 pipeline.py -subdir raw

notebook: setup
	jupyter notebook

tests: 
	cd $(PIPELINE_DIR) && pytest

lint:
	cd $(PIPELINE_DIR) && 

clean:
	rm -rf __pycache__
	rm -rf $(VENV)

