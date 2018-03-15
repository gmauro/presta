APPNAME=`cat APPNAME`
PRESTA_DIR=${HOME}/presta
TARGETS=all  build clean install uninstall

all:
	@echo "Try one of: ${TARGETS}"

build: clean dependencies
	python setup.py sdist
	python setup.py bdist_wheel

clean:
	python setup.py clean --all
	find . -name '*.pyc' -delete
	rm -rf dist *.egg-info __pycache__ build

config:
	mkdir -p ${PRESTA_DIR}
	if [ ! -f ${PRESTA_DIR}/presta_config.yml ]; then \
		cp presta/config/presta_config.yml ${PRESTA_DIR}; \
	fi

dependencies: requirements.txt
	pip install -r requirements.txt

install: build config
	pip install dist/*.whl

uninstall:
	pip uninstall -y ${APPNAME}
