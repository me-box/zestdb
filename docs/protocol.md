## Protocol

Both POST and GET messages are exchanged over a ZeroMQ Request-Reply socket using a CoAP-like message format. A message contains a header followed by an optional token, any message options then an optional payload. For example, a POST message must contain a header, options and a payload, whereas a GET message will not contain a payload. An acknowledgement may consist only of a header but can also consist of a header, options, and payload.

An OBSERVE message is slightly more complex and involves an initial Request-Reply exchange to setup communication over a Router-Dealer socket. This allows multiple clients to connect to the server and receive updates posted to an observed path. An OBSERVE message is implemented as a special type of GET message with an observe option set. The server replies to the GET with a UUID in the payload which is used to identify the client over the Router-Dealer communication.

### message structure

All values are in bits unless specified and all values are unsigned.

#### header

| code | oc | tkl                |
|------|----|--------------------|
|   8  |  8 | 16 (network order) |

* tkl = token length in bytes
* oc = number of options present
* code = CoAP specified

#### token (optional)

| token |
|-------|
| bytes |

#### options (repeating)

| number             | length             | value | ... |
|--------------------|--------------------|-------|-----|
| 16 (network order) | 16 (network order) | bytes | ... |

* number = CoAP specified
* value = CoAP specified

Some options must be present depending on the type of message.

*GET request*

* uri_path
* uri_host
* content_format

*GET request (observe)*

* uri_path
* uri_host
* content_format
* observe (contains string 'data' or '' or 'audit')
* max_age

*GET response*

* content_format

*GET response (observe)*

* content_format 
* public_key (number 2048, contains key)

*POST request*

* uri_path
* uri_host
* content_format

*POST response*


*DELETE request*

* uri_path
* uri_host
* content_format


*DELETE response*

#### message payload (optional)

| payload |
|---------|
|  bytes  |
