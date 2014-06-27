.PHONY: dev install test cover docs

PYTHON=`which python`
PIP=`which pip`
NOSE=`which nosetests`
VENV=`which virtualenv`

venv:
	${VENV} venv
	source venv/bin/activate

dev: venv
	${PIP} install nose coverage pylint sphinx
	${PYTHON} setup.py develop

install:
	${PYTHON} setup.py install

test:
	${NOSE} -v --with-doctest aiocouchdb

cover: test
	${NOSE} --with-coverage --cover-html --cover-erase --cover-package=aiocouchdb

docs:
	make -C docs -e PYTHONPATH=".." html
