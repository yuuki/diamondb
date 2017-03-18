DiamonDB: the rebuild of time series database
=============================================

[![Build Status](https://travis-ci.org/yuuki/diamondb.svg?branch=master)](https://travis-ci.org/yuuki/diamondb)
[![Go Report Card](https://goreportcard.com/badge/github.com/yuuki/diamondb)](https://goreportcard.com/report/github.com/yuuki/diamondb)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This software is still heavily under development.

# What is DiamonDB?

DiamonDB is a reliable, scalable, cloud-based time series database.

- __Reliablity on top of well-known storage implementations__
- __Horizontal scalability__
- __Cost optimized__
- __Compatible with Graphite API__
- __Written in Go__

# Development

## Run server with Docker

```shell
make up
```

## Build

```shell
make
```

## Test

```shell
make test
```

# Presentations (in Japanese)

[サーバモニタリング向け時系列データベースの探究 / The study of time-series database for server monitoring](https://speakerdeck.com/yuukit/the-study-of-time-series-database-for-server-monitoring)

# Thanks

[astj](https://github.com/astj), [itchyny](https://github.com/itchyny), [haya14busa](https://github.com/haya14busa)

# LICENSE

Copyright 2016 TSUBOUCHI, Yuuki <yuki.tsubo@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"): you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
