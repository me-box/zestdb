# zest

A REST over ZeroMQ.

#### Buiding

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

## Usage example

To get a complete list of options use the --help flag

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