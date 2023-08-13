# Guile Scheme PostgreSQL Language Extension (Scruple)

## Status

Nacent. Zero functionality.

## Overview

This project provides a PostgreSQL extension that enables the creation
of database functions using Guile Scheme. It allows developers to
write database functions in Scheme and execute them within the
PostgreSQL server.

## Features

- Define and run PostgreSQL functions in Guile Scheme
- Execute SQL queries from within Scheme code
- Comprehensive testing suite

## Requirements

- PostgreSQL 14
- Guile 3.0
- Ubuntu 22.04

## Development Setup

There are two development configurations currently supported, one in
docker and one not.  To set up your system to build and test scruple
on Ubuntu 22.04, run

``` bash
tools/dev-setup-ununtu-22-04.sh
```

to install Linux dependencies, and then

``` bash
tools/dev-setup-postgres-ubuntu-22-04.sh
```

to set up a database and configure Postgres.

If you'd like use docker, make sure that docker is installed on your
system and run

``` bash
tools/dev-setup-docker-postgres-ubuntu-22-04.sh
```

These scripts are all intended to be idempotent.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/imofftoseethewizard/scruple.git
```

2. Navigate to the project directory:
```bash
cd scruple
```

3. Build the extension:
```bash
make
```

4. Install the extension:
```bash
make install
```

## Usage

Once installed, you can use the extension within PostgreSQL by
creating functions in Guile Scheme. See the `doc/` directory for
detailed documentation and examples.

## Testing

Test code, scripts, and data are located in the `test/` directory. To
run the tests, execute:

```bash
make test
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

For questions, comments, or suggestions, please open an issue on GitHub.

## History

This project is a test of using ChatGPT on a green-field programming
task. It was started in early August of 2023.  So far, about 6 hours
of time has gone into this, and it's just a complicated version of
"Hello, World!" at this point.  The one useful bit currently preset is
that it could be used as a starting point for any Postgres extension.
