PROJECT=aiocouchdb
PYTHON=`which python`
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
	`which python3` -m venv venv
	@echo "Virtuanenv has been created. Don't forget to run . venv/bin/active"


.PHONY: dev
# target: dev - Setups developer environment
dev: venv
	${PIP} install nose coverage pylint flake8 sphinx
	${PYTHON} setup.py develop


.PHONY: install
# target: install - Installs aiocouchdb package
install:
	`which python3` setup.py install


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
check: flake
	${NOSE} --with-doctest ${PROJECT}


.PHONY: check-couchdb
# target: check-couchdb - Runs test suite against real CouchDB instance (AIOCOUCHDB_URL="http://localhost:5984")
check-couchdb: flake
	AIOCOUCHDB_URL="http://localhost:5984" AIOCOUCHDB_TARGET="couchdb" \
	${NOSE} --with-doctest ${PROJECT}


flake:
	${FLAKE8} --max-line-length=80 --statistics --exclude=tests --ignore=E501,F403 ${PROJECT}


.PHONY: cover
# target: cover - Generates coverage report
cover:
	${NOSE} --with-coverage --cover-html --cover-erase --cover-package=${PROJECT}


.PHONY: pylint
# target: lint - Runs pylint checks
pylint:
	${PYLINT} --rcfile=.pylintrc ${PROJECT}


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
