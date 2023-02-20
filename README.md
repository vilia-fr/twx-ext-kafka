# ThingWorx Kafka extension

Amazon Managed Streaming for Apache Kafka extension package.

Tested with ThingWorx 9.3.5 and AWS MSK with public access enabled. Authenticates via plain username + password over SASL / SCRAM. This extension can be used
for implementing prototypes and demos.

**WARNING: Please do not use it in production, the quality is not there yet**

## Usage

1. Import the extension (see Releases)
2. Create two Things based on `ConsumerTemplate` and `ProducerTemplate`
3. Configure those things with your access details. Note that your Consumer thing takes a comma-separated list of topics and will subscibe to all of them. The 
`topics` and `groupId` configuration parameters are ignored for Producers.
4. Subscribe to Consumer's `DataReceived` event
5. Use Producer's `Send` service to send data

## Issues and limitations

1. The fact that consumer starts a thread makes it likely incompatible with the clustered mode, although I didn't try that.
2. All payloads get (de)serialized from/to `String`.
3. Transactional mode is not implemented.
4. The messages are sent one-by-one, there's no batching.
5. The code needs to be de-duped.
6. There's no build script.
