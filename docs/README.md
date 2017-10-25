A CoAP inspired implementation of a RESTful-like experience implemented over ZeroMQ.

The current implementation supports POST/GET of JSON, text and binary data with backend storage implemented on top of a git-based file system. In additional to POST/GET the server allows a client to 'observe' a path to receive any POST updates.

An API exists to support key/value storage and retrieval as well as times series storage and retrieval which is specified as part of the path.

Access control is supported through macaroons which can be enabled using a command-line flag. A command-line tool is provided to help mint macaroons for testing.

The zest protocol is documented [here](protocol).

### Basic usage examples

You can run a server and test client using Docker. Each command supports --help to get a list of parameters.

#### starting server

```bash
$ docker run -p 5555:5555 -p 5556:5556 -d --name zest --rm jptmoore/zest /app/zest/server.exe --secret-key 'EKy(xjAnIfg6AT+OGd?nS1Mi5zZ&b*VXA@WxNLLE'
```

#### running client to post key/value data

```bash
$ docker run --network host -it jptmoore/zest /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --payload '{"name":"dave", "age":30}' --mode post
```

#### running client to get key/value data

```bash
$ docker run --network host -it jptmoore/zest /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode get
```

#### running client to observe changes to a resource path

```bash
$ docker run --network host -it jptmoore/zest /app/zest/client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode observe
```

#### generating a key pair

```bash
$ docker run -it zeromq/zeromq /usr/bin/curve_keygen
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


### More advanced usage (you need to build the code locally)

You can write a binary such as a image to the database:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode post --format binary --file --payload image.jpg
```

Reading an image from the database:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode get --format binary > /tmp/image.jpg
```

When you observe a path this functionality will expire. By default a single observation will take place and then expire immediately. To extend this behaviour you need to specify a 'max-age' flag. This is the time in seconds the observation will expire from its first invocation. For example to observe a path for 1 hour you could do the following:

```bash
$ client.exe --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode observe --max-age 3600 --loop 1000
```

The example above used the 'loop' flag which can be applied to any operation to control how many times it occurs. We can also add a 'freq' flag which controls the frequency of the loop. This is useful for generating performance tests such as:

```bash
$ client.exe  --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode post --format text --payload 'hello world' --loop 10 --freq 0.001
```


### Security

All communication is encrypted using ZeroMQ's built-in CurveZMQ security. However, access to the server can be controlled through tokens called macaroons. A command-line utility exists to mint macaroons which restricts what path can be accessed, who is accessing it and what the operation is. For example to mint a macaroon to control a POST operations you could do:

```bash
$ mint.exe --path 'path = /kv/foo' --method 'method = POST' --target 'target = Johns-MacBook-Pro.local' --key 'secret'
```

In the above example we are allowing POST operations to the path '/kv/foo' for a host called 'Johns-MacBook-Pro.local'. The output from this command is a token which can be specified on the command-line as follows:

```bash
$ client.exe  --server-key 'vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<' --path '/kv/foo' --mode post --format text --payload 'hello world' --token 'MDAwZWxvY2F0aW9uIAowMDEwaWRlbnRpZmllciAKMDAxN2NpZCBwYXRoID0gL2t2L2ZvbwowMDE2Y2lkIG1ldGhvZCA9IFBPU1QKMDAyOWNpZCB0YXJnZXQgPSBKb2hucy1NYWNCb29rLVByby5sb2NhbAowMDJmc2lnbmF0dXJlIJKloR0-WbbJBV1gXPWGimpo_eTByptDAIZ2wh1bZfKMCg=='
```

If we started our server using the same token key we will be able to verify the above request as being a valid operation. So in this case we would have started our server as follows:

```bash
$ server.exe --secret-key 'EKy(xjAnIfg6AT+OGd?nS1Mi5zZ&b*VXA@WxNLLE' --enable-logging --token-key 'secret'
```

In the above example, we have turned debugging on which is useful option if you want to write your own client. A client can also be run in this mode.