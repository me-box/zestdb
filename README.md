# REST over ZeroMQ.

A CoAP inspired implementation of a RESTful-like experience implemented over ZeroMQ in ReasonML.

The current implementation supports POST/GET of JSON data with backend storage implemented on top of a git-based file system. In additional to POST/GET the server allows a client to 'observe' a path to receive any POST updates.

An API exists to support key/value storage and retrieval as well as times series storage and retrieval.

### Building

#### to fix an issue with .pub being a reserved word in ReasonML

```bash
git clone https://github.com/jptmoore/ocaml-zmq.git
cd ocaml-zmq
opam pin add -y zmq .
opam install lwt-zmq
```

#### to build the server

```bash
cd src
make
make install
```

#### to build the test client

```bash
cd test
make
make install
```

#### to build the genkeys utility

```bash
cd utils/genkeys
make
make install
```

### Usage example

To get a complete list of options use the --help flag

#### to generate a new public and private key

```bash
/tmp/genkeys
```

#### starting server locally

```bash
$ /tmp/server.exe --secret-key 'uf4XGHI7[fLoe&aG1tU83[ptpezyQMVIHh)J=zB1' --enable-logging
```

#### running client to post key/value data

```bash
$ /tmp/client.exe --server-key 'qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s?' --path '/kv/foo' --payload '{"name":"fred", "age":30}' --mode post
```

#### running client to get key/value data

```bash
$ /tmp/client.exe --server-key 'qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s?' --path '/kv/foo' --mode get
```

#### running client to observe changes to a resource path

```bash
$ /tmp/client.exe --server-key 'qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s?' --path '/kv/foo' --mode observe
```