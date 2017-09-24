# REST over ZeroMQ

A CoAP inspired implementation of a RESTful-like experience implemented over ZeroMQ in ReasonML.

The current implementation supports POST/GET of JSON data with backend storage implemented on top of a git-based file system. In additional to POST/GET the server allows a client to 'observe' a path to receive any POST updates.

An API exists to support key/value storage and retrieval as well as times series storage and retrieval which is specified as part of the path.

**Todo:** 

* Token-based control of actions such as being able to observe a resource for fixed time periods or a number of times of access etc. This will utilise the in-built HyperCat to describe and control what is in the backend storage and what can be accessed.

* Factor out functionality into a stand-alone library and support other languages.

### Building

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

### Usage example

* To get a complete list of options of the server or test client use the --help flag

* Keys can be generated using the ZeroMQ utility 'curve_keygen'

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

### Key/Value API

#### Write entry
    URL: /kv/<key>
    Method: POST
    Parameters: JSON body of data, replace <id> with an identifier
    Notes: add data to time series with given identifier

#### Read latest entry
    URL: /kv/<key>/latest
    Method: GET
    Parameters: replace <id> with an identifier
    Notes: return the latest entry

### Time series API

#### Write entry
    URL: /ts/<id>
    Method: POST
    Parameters: JSON body of data, replace <id> with an identifier
    Notes: add data to time series with given identifier

#### Read latest entry
    URL: /ts/<id>/latest
    Method: GET
    Parameters: replace <id> with an identifier
    Notes: return the latest entry
    
#### Read last number of entries
    
    URL: /ts/<id>/last/<n>
    Method: GET
    Parameters: replace <id> with an identifier, replace <n> with the number of entries
    Notes: return the number of entries requested
    
#### Read all entries since a time
    
    URL: /ts/<id>/since/<from>
    Method: GET
    Parameters: replace <id> with an identifier, replace <from> with epoch seconds
    Notes: return the number of entries from time provided
    
#### Read all entries in a time range
    
    URL: /ts/<id>/range/<from>/<to>
    Method: GET
    Parameters: replace <id> with an identifier, replace <from> and <to> with epoch seconds
    Notes: return the number of entries in time range provided