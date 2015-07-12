PROJECT=aiocouchdb
PYTHON=`which python3`
PIP=`which pip`
NOSE=`which nosetests`
PYLINT=`which pylint`
FLAKE8=`which flake8`
SPHINX=`which sphinx-build`


.PHONY: help
# target: help - Prints this help
help:
	@egrep "^# target:" Makefile | sed -e 's/^# target: //g' | sort


.PHONY: venv
# target: venv - Setups virtual environment
venv:
	${PYTHON} -m venv venv
	@echo "Virtuanenv has been created. Don't forget to run . venv/bin/active"


.PHONY: dev
# target: dev - Setups developer environment
dev:
	${PIP} install nose coverage pylint flake8 sphinx
	${PYTHON} setup.py develop


.PHONY: install
# target: install - Installs aiocouchdb package
install:
	${PYTHON} setup.py install


.PHONY: clean
# target: clean - Removes intermediate and generated files
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf cover
	make -C docs clean
	python setup.py clean


.PHONY: purge
# target: purge - Removes all unversioned files and resets repository
purge:
	git reset --hard HEAD
	git clean -xdff


.PHONY: check
# target: check - Runs test suite against mocked environment
check: flake pylint-errors
	${NOSE} --with-doctest ${PROJECT}


.PHONY: check-couchdb
# target: check-couchdb - Runs test suite against real CouchDB instance (AIOCOUCHDB_URL="http://localhost:5984")
check-couchdb: flake
	AIOCOUCHDB_URL="http://localhost:5984" AIOCOUCHDB_TARGET="couchdb" \
	${NOSE} --with-doctest ${PROJECT}


.PHONY: distcheck
# target: distcheck - Checks if project is ready to ship
distcheck: distcheck-clean distcheck-33 distcheck-34

distcheck-clean:
	rm -rf distcheck

distcheck-33:
	mkdir -p distcheck
	virtualenv --python=python3.3 distcheck/venv-3.3
	distcheck/venv-3.3/bin/python setup.py install
	distcheck/venv-3.3/bin/python setup.py test
	AIOCOUCHDB_URL="http://localhost:5984" AIOCOUCHDB_TARGET="couchdb" \
		distcheck/venv-3.3/bin/python setup.py test

distcheck-34:
	mkdir -p distcheck
	python3.4 -m venv distcheck/venv-3.4
	distcheck/venv-3.4/bin/python setup.py install
	distcheck/venv-3.4/bin/python setup.py test
	AIOCOUCHDB_URL="http://localhost:5984" AIOCOUCHDB_TARGET="couchdb" \
		distcheck/venv-3.4/bin/python setup.py test

flake:
	${FLAKE8} --max-line-length=80 --statistics --exclude=tests --ignore=E501,F403 ${PROJECT}


.PHONY: cover
# target: cover - Generates coverage report
cover:
	${NOSE} --with-coverage --cover-html --cover-erase --cover-package=${PROJECT}


.PHONY: pylint
# target: pylint - Runs pylint checks
pylint:
	${PYLINT} --rcfile=.pylintrc ${PROJECT}


.PHONY: pylint-errors
# target: pylint-errors - Reports about pylint errors
pylint-errors:
	${PYLINT} --rcfile=.pylintrc -E ${PROJECT}


.PHONY: docs
# target: docs - Builds Sphinx html docs
docs:
	${SPHINX} -b html -d docs/_build/doctrees docs/ docs/_build/html


.PHONY: release
# target: release - Yay new release!
release: ${PROJECT}/version.py
	sed -i s/\'dev\'/\'\'/ $<
	git commit -m "Release `${PYTHON} $<` version" $<


.PHONY: pypi
# target: pypi - Uploads package on PyPI
pypi:
	${PYTHON} setup.py sdist register upload
