# PostgreSQL/Guile3 Trusted Language Extension

This project joins my favorite database to my favorite programming
language.

## Status

Alpha.  Plguile3 is feature complete.  It has a full functional test
suite and there are no known functional defects.  The latest version
will generally be the most stable.  Current work is focusing on
refinement and documentation of the code.

## Overview

This project provides a PostgreSQL trusted language extension that
enables the creation of database functions using Guile Scheme 3.0.

## Features

- Define and run functions or call inline with `DO ...`
- All Scheme code is evaluated in a sandbox module
- Per user execution environments
- Per user call time and allocation limits (within each connection)
- Supports all built-in data types
- Triggers and Event Triggers
- Comprehensive testing suite

## Roadmap

The scope of the 1.0 release is detailed below.

The 0.21.x versions will reorganize, refactor, refine, and document
plguile3.c and plguile3.scm.

The 0.23.x versions will include an extensive test campaign covering
load, concurrency, session isolation, memory correctness, fuzzing,
etc.  Tests will also be extended to Postgres version 12-16.

Version 0.24.0 will be the pre-1.0 beta release.

The 0.24.x versions will include

- documentation and examples
- support for Postgres 12, 13, 15, and 16
- packaging for pgxn, deb, rpm, and apk.

The 0.25.x releases will include extensions and usability updates
after use in at least one large project.

Post 1.0 improvements:

The 1.1.x versions will adapt Racket's SQL interface.

The 1.3.x versions will add support for the foreign data wrapper
interface.

The ?.?.x versions will add support for parse tree representation
in event trigger handlers.

## Requirements

- PostgreSQL 14
- Guile 3.0
- Ubuntu 22.04

## Packaging

https://tembo.io/blog/pgxn-ecosystem-jobs

## Development Setup

Install docker.  To create the test image for Postgres 14

    test/build-container-image 14

To start a test monitor running that image

    test/start-test-container 14 && docker logs -f plguile3-test-14

That starts the Postgres cluster, builds and installs `pgTAP`
and `plguile3` and then enters an infinite loop containing
`inotifywait` watching a few key dependencies. When one of those
changes, it rebuilds and reinstalls `plguile3` and then calls
`pg_prove` on the tests in `test/tests.sql`.

## Installation

For now, it's just a source install.

```bash
git clone https://github.com/imofftoseethewizard/plguile3.git
cd plguile3
make
sudo make install
```

I'll put this on `pgxn` at some point in the future.

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
task. It was started in early August of 2023.
