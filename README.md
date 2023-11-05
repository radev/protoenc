# protoenc

Protobuf encoder for serializing messages in a format compatible with the
Confluent wire format.

## Usage

```go
// First you construct encoder which would register schema for the message (you can pass empty message)
enc, err := protoenc.NewEncoder(context.Background(), client, subject, (*test.Test)(nil))
...
// Then to serialize message you call Marshal method
bytes, err := enc.Marshal(&test.Test{Id: "1234"})
```
