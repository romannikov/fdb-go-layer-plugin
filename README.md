# FoundationDB Go Layer Plugin

A `protoc` plugin that generates Go repository code for interacting with FoundationDB, based on annotated Protobuf definitions.

## Overview

The FoundationDB Go Layer Plugin simplifies the process of creating Go repositories that interact with FoundationDB. By annotating your Protobuf messages with custom options, you can automatically generate repository code that handles common CRUD operations, including support for primary keys and secondary indexes.

## Features

-   Automatic Repository Generation: Generate Go code for repositories based on your Protobuf message definitions.
-   Primary Key Support: Define primary keys using custom annotations in your .proto files.
-   Secondary Indexes: Add secondary indexes to your messages for efficient querying.
-   FoundationDB Integration: Generated code uses the FoundationDB Go bindings for database operations.
-   Transaction Management: Supports both automatic and manual transaction management.
-   Customizable and Extensible: Extend and customize the generated code without modifying the generated files.

## Installation

### Prerequisites
-   Go 1.18 or later
-   `protoc` compiler (version compatible with your Protobuf definitions)
-   FoundationDB installed and configured
-   `protoc-gen-go` and `protoc-gen-go-grpc` plugins installed

### Install the Plugin
You can install the plugin using go install:
```
go install github.com/romannikov/fdb-go-layer-plugin@latest
```
Ensure that your GOPATH/bin is in your PATH so that protoc can find the plugin:
```
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Usage

### Define Your Protobuf Messages
Create your `.proto` files with custom annotations for primary keys and secondary indexes.

Example `user.proto`:
```
syntax = "proto3";

package myapp;

option go_package = "github.com/yourusername/yourproject/pb;pb";

import "fdb-go-layer/annotations.proto";

message User {
  option (annotations.primary_key) = "id";
  option (annotations.secondary_index) = { fields: "email" };

  int64 id = 1;
  string name = 2;
  string email = 3;
}
```
### Generate Code
Run the `protoc` compiler with the plugin to generate Go code for your messages and repositories.
```
protoc \
  --plugin=protoc-gen-fdb-go-layer-plugin=./fdb-go-layer-plugin \
  --fdb-go-layer-plugin_out=. \
  --go_out=. \
  --go_opt=paths=source_relative \
  user.proto
```
