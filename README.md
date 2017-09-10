# zest

A REST over ZeroMQ server based on CoAP.

#### to fix an issue with .pub being a reserved word in reason

```bash
git clone https://github.com/jptmoore/ocaml-zmq.git
cd ocaml-zmq
opam pin add -y zmq .
opam install lwt-zmq
```


## Usage example

To get a complete list of options use the --help flag

#### starting server

```bash
$ server.exe --secret-key 'uf4XGHI7[fLoe&aG1tU83[ptpezyQMVIHh)J=zB1' --enable-logging
```

#### running client to post key/value data

```bash
$ client.exe --server-key 'qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s?' --public-key 'MP9pZzG25M2$.a%[DwU$OQ#-:C}Aq)3w*<AY^%V{' --secret-key 'j#3yqGG17QNTe(g@jJt6[LOg%ivqr<:}L%&NAUPt' --path '/kv/foo' --payload '{"name":"fred", "age":30}' --mode post --format jso
```

#### running client to observe changes to a resource path

```bash
$ client.exe --server-key 'qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s?' --public-key 'MP9pZzG25M2$.a%[DwU$OQ#-:C}Aq)3w*<AY^%V{' --secret-key 'j#3yqGG17QNTe(g@jJt6[LOg%ivqr<:}L%&NAUPt' --path '/kv/foo' --mode observe
```