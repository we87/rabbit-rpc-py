build: clean
	poetry build --format=sdist

clean:
	rm -rf dist