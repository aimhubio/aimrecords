# AimRecords - Record-oriented data storage

![GitHub Top Language](https://img.shields.io/github/languages/top/aimhubio/aimrecords)
[![PyPI Package](https://img.shields.io/pypi/v/aimrecords?color=yellow)](https://pypi.org/project/aimrecords/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)


Library to effectively store the tracked experiment logs.

See the documentation [here](docs/index.md).

## Getting Started

These instructions will get you a copy of the project up and running on your 
local machine for development and testing purposes.

### Requirements

* Python 3

We suggest to use [virtual
environment](https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments) for managing local dependencies.

To start development first install all dependencies:

```bash
pip install -r requirements.txt
```

### Project Structure

```
├── aimrecords  <-----------  main project code
│   ├── artifact_storage  <-  manage storage of artifacts
│   └── record_storage  <---  manage records storage of a single artifact
├── docs  <-----------------  data format documentation
├── examples  <-------------  example usages of aimrecords
└── tests
```

## Running the tests

Run tests via command `pytest` in the root folder.

### Code Style
We follow [pep8](https://www.python.org/dev/peps/pep-0008/) style guide 
for python code.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our 
code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available,
see the [tags on this repository](https://github.com/aimhubio/aim/tags).

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
