PRESTA_DIR=${HOME}/presta
TARGETS=all install build clean uninstall

all:
	@echo "Try one of: ${TARGETS}"

install: build config
	pip install dist/*.whl

build: clean
	python setup.py bdist_wheel

clean:
	python setup.py clean --all
	find . -regex '.*\(\.pyc\|\.pyo\)' -exec rm -fv {} \;
	rm -rf dist *.egg-info

config:
	mkdir -p ${PRESTA_DIR}
	if [ ! -f ${PRESTA_DIR}/presta_config.yml ]; then \
		cp presta/config/presta_config.yml ${PRESTA_DIR}; \
	fi

uninstall:
	pip uninstall -y presta
