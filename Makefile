APPNAME=`cat APPNAME`
TARGETS=all build clean dependencies install tag uninstall
VERSION=`cat VERSION`

all:
	@echo "Try one of: ${TARGETS}"

build: clean dependencies
	python setup.py sdist
	python setup.py bdist_wheel

clean:
	python setup.py clean --all
	find . -name '*.pyc' -delete
	rm -rf dist *.egg-info __pycache__ build

dependencies: requirements.txt
	pip install -r requirements.txt

install: build
	pip install dist/*.whl

tag:
	git tag v${VERSION}

uninstall:
	pip uninstall -y ${APPNAME}
