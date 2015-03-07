PROJECT=aiocouchdb
PYTHON=`which python`
PIP=`which pip`
NOSE=`which nosetests`
VENV=`which virtualenv`


.PHONY: help
# target: help - Prints this help
help:
	@egrep "^# target:" Makefile | sed -e 's/^# target: //g' | sort


.PHONY: venv
# target: venv - Setups virtuanenv
venv:
	${VENV} venv
	source venv/bin/activate


.PHONY: dev
# target: dev - Setups developer environment
dev: venv
	${PIP} install nose coverage pylint sphinx
	${PYTHON} setup.py develop


.PHONY: install
# target: install - Installs aiocouchdb package
install:
	${PYTHON} setup.py install


.PHONY: check
# target: check - Runs test suite against mocked environment
check:
	${NOSE} --with-doctest ${PROJECT}


.PHONY: check-couchdb
# target: check-couchdb - Runs test suite against real CouchDB instance (AIOCOUCHDB_URL="http://localhost:5984")
check-couchdb:
	AIOCOUCHDB_URL="http://localhost:5984" AIOCOUCHDB_TARGET="couchdb" \
	${NOSE} --with-doctest ${PROJECT}


.PHONY: cover
# target: cover - Generates coverage report
cover:
	${NOSE} --with-coverage --cover-html --cover-erase --cover-package=${PROJECT}


.PHONY: docs
# target: docs - Builds Sphinx html docs
docs:
	make -C docs -e PYTHONPATH=".." html
