IS_VENV_ACTIVE=false
ifdef VIRTUAL_ENV
	IS_VENV_ACTIVE=true
endif

enforce_venv:
ifeq ($(IS_VENV_ACTIVE), false)
	$(error "You must activate your virtual environment. Exiting...")
endif

venv:
	python -m venv .venv

require: enforce_venv requirements.txt
	python -m pip install -r requirements.txt

run: enforce_venv
	flask run --debug

test: enforce_venv
	python checker/checker.py

