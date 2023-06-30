# :lock_with_ink_pen: `dhstore`

[![Go Test](https://github.com/ipni/dhstore/actions/workflows/go-test.yml/badge.svg)](https://github.com/ipni/dhstore/actions/workflows/go-test.yml)

A Service to store double hashed indexed records with their corresponding encrypted values backed by
Pebble key-value store according to
the [IPNI Reader Privacy specification](https://github.com/ipni/specs/pull/5).

The service exposes a HTTP API that allows clients to `PUT` or `GET` encrypted index value keys, as
well as encrypted IPNI metadata.

## Usage

The repository provides a CLI command to start up the `dhstore` service, with following usage:

```shell
$ dhstore -h
Usage of dhstore
  -disableWAL
        Weather to disable WAL in Pebble dhstore.
  -listenAddr string
        The dhstore HTTP server listen address. (default "0.0.0.0:40080")
  -logLevel string
        The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset. (default "info")
  -providersURL
        Providers URL to enable dhfind.
  -storePath string
        The path at which the dhstore data persisted. (default "./dhstore/store")
```

## Run Server Locally

To run the server locally, execute:

```shell
$ go run cmd/dhstore/main.go
```

The above command starts the HTTP API exposed on default listen address: `http://localhost:40080`

For more information

## Install

To install `dhstore` CLI directly via Golang, run:

```shell
$ go install github.com/ipni/dhstore/cmd/dhstore@latest
```

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
