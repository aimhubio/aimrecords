# Tests
## Run Tests
To run tests, run `python3 -m unittest discover -s tests` in the root folder. 

## Create Tests
We use `unittest` to write out tests. To enable discovery, ensure the following requirements are met:
- Ensure that all tests files start with the word `test`. 
- Must include `__init__.py` in every test folders.
- The test class needs to inherit from `unittest.TestCase`.