# :lock_with_ink_pen: `dhstore`

[![Go Test](https://github.com/ipni/dhstore/actions/workflows/go-test.yml/badge.svg)](https://github.com/ipni/dhstore/actions/workflows/go-test.yml)

A Service to store double hashed indexed records with their corresponding encrypted values backed by
Pebble key-value store according to
the [IPNI Reader Privacy specification](https://github.com/ipni/specs/pull/5).

The service exposes a HTTP API that allows clients to `PUT` or `GET` encrypted index value keys and encrypted IPNI metadata.

## Usage

The repository provides a CLI command to start up the `dhstore` service, with following usage:

```shell
$ dhstore -h
Usage of ./dhstore:
  -blockCacheSize string
    	Size of pebble block cache. Can be set in Mi or Gi. (default "1Gi")
  -disableWAL
    	Weather to disable WAL in Pebble dhstore.
  -experimentalCompactionDebtConcurrency string
    	CompactionDebtConcurrency controls the threshold of compaction debt at which additional compaction concurrency slots are added. For every multiple of this value in compaction debt bytes, an additional concurrent compaction is added. This works "on top" of L0CompactionConcurrency, so the higher of the count of compaction concurrency slots as determined by the two options is chosen. Can be set in Mi or Gi. (default "1Gi")
  -experimentalL0CompactionConcurrency int
    	The threshold of L0 read-amplification at which compaction concurrency is enabled (if CompactionDebtConcurrency was not already exceeded). Every multiple of this value enables another concurrent compaction up to MaxConcurrentCompactions. (default 10)
  -formatMajorVersion int
    	Sets the format of pebble on-disk files. Unset or 0 uses the current version. Latest supported version is 16
  -l0CompactionFileThreshold int
    	The count of L0 files necessary to trigger an L0 compaction. (default 500)
  -l0CompactionThreshold int
    	The amount of L0 read-amplification necessary to trigger an L0 compaction. (default 2)
  -l0StopWritesThreshold int
    	Hard limit on Pebble L0 read-amplification. Writes are stopped when this threshold is reached. (default 12)
  -listenAddr string
    	The dhstore HTTP server listen address. (default "0.0.0.0:40080")
  -logLevel string
    	The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset. (default "info")
  -maxConcurrentCompactions int
    	Specifies the maximum number of concurrent Pebble compactions. As a rule of thumb set it to the number of the CPU cores. (default 10)
  -metricsAddr string
    	The dhstore metrics HTTP server listen address. (default "0.0.0.0:40081")
  -providersURL value
    	Providers URL to enable dhfind. Multiple OK
  -storePath string
    	The path at which the dhstore data persisted. (default "./dhstore/store")
  -storeType pebble
    	The store type to use. only pebble and `fdb` is supported. Defaults to `pebble`. When `fdb` is selected, all `fdb*` args must be set. (default "pebble")
  -version
    	Show version information,
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

To install `dhstore` with FDB support, run:

```shell
$ go install -tags fdb github.com/ipni/dhstore/cmd/dhstore@latest
```

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
