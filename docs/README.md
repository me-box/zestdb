### Introduction

A [CoAP](https://tools.ietf.org/html/rfc7252) inspired implementation of a RESTful-like experience implemented over [ZeroMQ](http://zeromq.org/).

The current implementation supports POST/GET of JSON, text and binary data with backend storage implemented on top of a git-based file system. In additional to POST/GET the server allows multiple clients to 'observe' a path to directly receive any data POSTed to specific paths. Communication can take place over TCP or over Interprocess communication (IPC).

Data stored can be described and queried using a built in [HyperCat](http://www.hypercat.io/).

An API exists to support key/value storage and retrieval as well as times series storage and retrieval which is specified as part of the path.

Access control is supported through [macaroons](https://github.com/rescrv/libmacaroons) which can be enabled using a command-line flag. A command-line tool is provided to help mint macaroons for testing.

The zest protocol is documented [here](protocol).

### Basic usage examples

You can run a server and test client using [Docker](https://www.docker.com/). Each command supports --help to get a list of parameters.

#### starting server

```bash
$ docker run -p 5555:5555 -p 5556:5556 -d --name zest --rm jptmoore/zestdb /app/zest/server.exe --secret-key-file example-server-key
```

#### running client to post key/value data

```bash
$ docker run --network host -it jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo/bar' --payload '{"name":"dave", "age":30}' --mode post
```

#### running client to get key/value data

```bash
$ docker run --network host -it jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo/bar' --mode get
```

### Observation

One benefit of the Zest protocol being built on top of ZeroMQ means that it is easy to support features such as observing data written or read from the server in real-time. There are two types of observation modes: data and audit which provide data in a simple space-separated meta-format. Observing data is used to get a copy of what is POSTed to a specific path, whereas an audit request can be used to provide meta-data on a POST or GET containing information such as the hostnames involved and the type of query etc.

A typical use case for observation might consist of multiple deployed servers that you need to monitor from a single client. The client could make individual observation requests to each server and collate the data received in real-time to display on a dashboard.

#### running client to observe data POSTed to a resource path

```bash
$ docker run --network host -it jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo/bar' --mode observe
```

The above will produce data written in a format such as:

```bash
#timestamp #uri-path #content-format #data
1521554211213 /kv/foo/bar json {"room": "lounge", "value": 1} 
```

#### running client to observe audit information at a resource path

```bash
$ docker run --network host -it jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo/bar' --mode observe --observe-mode audit
```

The above will produce data written in a format such as:

```bash
#timestamp #server-name #host-name #operation #uri-path
1521553488680 Johns-MacBook-Pro.local Johns-MacBook-Pro.local POST /kv/foo/bar
```

As well as observing exact paths it is possible to use wildcard paths to receive information on a range of paths.

#### running client to observe audit information using a wildcard path

```bash
$ docker run --network host -it jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo/*' --mode observe --observe-mode audit
```

The example above is similar to the previous example but this time we will also receive audit information on any path that starts with '/kv/foo/'.


### Key/Value API

A value is uniquely identified by an id and key pair. For example, you might write a value to id='lounge' with key='lightbulb'.

#### Write entry
    URL: /kv/<id>/<key>
    Method: POST
    Parameters: JSON body of data, replace <id> and <key> with a string
    Notes: store data using given key
    
#### Read entry
    URL: /kv/<id>/<key>
    Method: GET
    Parameters: replace <id> and <key> with a string
    Notes: return data for given key    


#### List keys
    URL: /kv/<id>/keys
    Method: GET
    Parameters: replace <id> with a string
    Notes: return keys for given key id 

### Time series API

The time series API has support for writing generic JSON blobs or data in a specific format which allows extra functionality such as filtering and aggregation on the data. The generic blob API is called using the '/ts/blob' extension in the path. Otherwise it is assumed that the data consists of a value together with an optional tag. A value is integer or floating point number and a tag is an identifier with corresponding string value. For example:```{"room": "lounge", "value": 1}```. Tagging a value provides a way to group values together when accessing them. In the example provided you could retrieve all values that are in a room called 'lounge'. 

Data returned from a query is a JSON dictionary containing a timestamp in epoch milliseconds and the actual data. For example:```{"timestamp":1513160985841,"data":{"foo":"bar","value":1}}```. Data can also be aggregated by applying functions across values. This results in a response of a single value. For example: ```{"result":1}```. 


#### Write entry (auto-generated time)
    URL: /ts/<id>
    Method: POST
    Parameters: JSON body of data, replace <id> with a string
    Notes: add data to time series with given identifier (a timestamp will be calculated at time of insertion)
    
#### Write entry (user-specified time)
    URL: /ts/<id>/at/<t>
    Method: POST
    Parameters: JSON body of data, replace <id> with a string and <t> with epoch milliseconds
    Notes: add data to time series with given identifier at the specified time

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
    

#### Read first entry
    URL: /ts/<id>/first
    Method: GET
    Parameters: replace <id> with an identifier
    Notes: return the first entry
    
#### Read first number of entries
    
    URL: /ts/<id>/first/<n>
    Method: GET
    Parameters: replace <id> with an identifier, replace <n> with the number of entries
    Notes: return the number of entries requested    
    
#### Read all entries since a time (inclusive)
    
    URL: /ts/<id>/since/<from>
    Method: GET
    Parameters: replace <id> with an identifier, replace <from> with epoch milliseconds
    Notes: return the number of entries from time provided
    
#### Read all entries in a time range (inclusive)
    
    URL: /ts/<id>/range/<from>/<to>
    Method: GET
    Parameters: replace <id> with an identifier, replace <from> and <to> with epoch milliseconds
    Notes: return the number of entries in time range provided
    

#### Length of time series

    URL: /ts/<id>/length
    Method: GET
    Parameters: replace <id> with an identifier
    Notes: return the number of entries in the time series

    
#### Filtering
    
Filtering is an extension of the API path applied to tags to restrict the values returned in the format of ```/ts/.../filter/<tag_name>/<equals|contains>/<tag_value>``` where 'equals' is an exact match and 'contains' is a substring match. This feature is not available for '/ts/blob' data.


#### Aggregation

Aggregation is an extension of the API path to carry out functions on an array of values in the format of ```/ts/.../<sum|count|min|max|mean|median|sd>```. This feature is not available for '/ts/blob' data.
   

#### Complex queries

By combining both filtering and aggregation it is possible to produce more complex queries. For example:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/ts/sensor/last/100/filter/room/equals/lounge/max' --mode get
```

might provide the maximum value of a sensor located in a specific room based on the last 100 entries.

### Interprocess communication (IPC)

Interprocess communication (IPC) can take place between Docker containers. As with TCP communication, the server uses two endpoints. One for request/replies and one for broadcasting messages to observing peers.

#### starting server

```bash
$ docker run -v /tmp:/tmp --ipc=host -d --name zest --rm jptmoore/zestdb /app/zest/server.exe --secret-key-file example-server-key --request-endpoint 'ipc:///tmp/request' --router-endpoint 'ipc:///tmp/router'
```

#### observing

We need to use both endpoints when observing a path.

```bash
$ docker run -v /tmp:/tmp --ipc=host jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L44/W[wXL3<' --path '/kv/foo' --mode observe --request-endpoint 'ipc:///tmp/request' --router-endpoint 'ipc:///tmp/router'
```

#### posting

We only need one endpoint to post or get data.

```bash
$ docker run -v /tmp:/tmp --ipc=host jptmoore/zestdb /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --payload '{"name":"dave", "age":30}' --mode post --request-endpoint 'ipc:///tmp/request'
```


### Security

All communication is encrypted using ZeroMQ's built-in [CurveZMQ](http://curvezmq.org/) security. A client and server require a key pair which can be generated as follows:

```bash
$ docker run -it zeromq/zeromq /usr/bin/curve_keygen
```

However, access to the server can be controlled through tokens called macaroons. A command-line utility exists to mint macaroons which restricts what path can be accessed, who is accessing it and what the operation is. For example to mint a macaroon to control a POST operations you could do:

```bash
$ mint.exe --path 'path = /kv/foo' --method 'method = POST' --target 'target = Johns-MacBook-Pro.local' --key 'secret'
```

In the above example we are allowing POST operations to the path '/kv/foo' for a host called 'Johns-MacBook-Pro.local'. Wildcards are supported in caveats so we could, for example, specify any host using 'target = *' instead of the exact host. Wildcards are useful also for giving access to a range of paths. The output from this command is a token which can be specified on the command-line as follows:

```bash
$ client.exe  --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode post --format text --payload 'hello world' --token 'MDAwZWxvY2F0aW9uIAowMDEwaWRlbnRpZmllciAKMDAxN2NpZCBwYXRoID0gL2t2L2ZvbwowMDE2Y2lkIG1ldGhvZCA9IFBPU1QKMDAyOWNpZCB0YXJnZXQgPSBKb2hucy1NYWNCb29rLVByby5sb2NhbAowMDJmc2lnbmF0dXJlIJKloR0-WbbJBV1gXPWGimpo_eTByptDAIZ2wh1bZfKMCg=='
```

If we started our server using the same token key we will be able to verify the above request as being a valid operation. So in this case we would have started our server as follows:

```bash
$ server.exe --secret-key-file example-server-key --token-key-file example-token-key --enable-logging
```

In the above example, we have turned debugging on which is useful option if you want to write your own client. A client can also be run in this mode.

### More advanced usage

To add an entry to the in-built HyperCat:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/cat' --mode post --file --payload item1.json
```

To query the in-built HyperCat:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/cat' --mode get
```

You can write a binary such as a image to the database:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode post --format binary --file --payload image.jpg
```

Reading an image from the database:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode get --format binary > /tmp/image.jpg
```

When you observe a path this functionality will expire. By default a observation will last 60 seconds. To change this behaviour you need to specify a 'max-age' flag providing the number of seconds to observe for. For example to observe a path for 1 hour you could do the following:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode observe --max-age 3600
```

To carry out some performance tests we can use the 'loop' flag with POST and GET operations to control how many times they repeat. We can also add a 'freq' flag to control the frequency of the loop:

```bash
$ client.exe  --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode post --format text --payload 'hello world' --loop 10 --freq 0.001
```
