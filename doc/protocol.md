### message structure

All values are in bits unless specified.

#### header
| version  | tkl | oc | code |
| :--: | :--: |  :--: | :--: |
| 4 | 16 (network order) | 4 | 8 |

* tlk = token length in bytes
* oc = number of options present

#### token (optional)
| token |
| :--: |
| bytes |
#### options (repeating)
| number  | length | pad | value | ... | 
| :--: | :--: |  :--: | :--: | :--: | 
| 4 | 16 (network order) | 4 | bytes | ... |
#### message payload (optional)
| payload |
| :--: |
| bytes |