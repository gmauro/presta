TARGETS=all install install_user clean uninstall

all:
	@echo "Try one of: ${TARGETS}"

install: build
	pip install dist/*.whl

build: clean
	python setup.py bdist_wheel

clean:
	python setup.py clean --all
	find . -regex '.*\(\.pyc\|\.pyo\)' -exec rm -fv {} \;
	rm -rf dist *.egg-info

uninstall:
	pip uninstall -y presta