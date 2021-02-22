const { KinesisClient, CreateStreamCommand, PutRecordCommand, DescribeStreamCommand } = require("@aws-sdk/client-kinesis");
const axios = require('axios');

console.log('producer started');

const healthcheck = () => axios.get('http://localstack:4566/health?reload');

const kinesisReady = new Promise((resolve, reject) => {
    const func = () => setTimeout(() => healthcheck().then(({ data: { services: { kinesis } } }) => {
        if (kinesis == "running") {
            console.log("kinesis is ready");
            resolve();
        } else {
            console.log('waiting for kinesis');
            func();
        }
    })
    .catch(() => {
        console.log('waiting for kinesis');
        func();
    }), 2000);

    func();
});

const waitForStream = async (client, streamName) => {
    return new Promise((resolve, reject) => {
        const func = () => setTimeout(() => {
            console.log('waiting for stream');

            client.send(new DescribeStreamCommand({ StreamName: streamName }))
                .then(({ StreamDescription: { StreamStatus, StreamARN } }) => {
                    if (StreamStatus == "ACTIVE") {
                        resolve(StreamARN);
                    } else {
                        func();
                    }
                })
                .catch(() => func());
        }, 2000);

        func();
    });
};

const sendMessage = async (client, streamName, partitionKey, message) => {
    const func = () => setTimeout(() => {
        console.log('sending message');

        client.send(new PutRecordCommand({ 
                StreamName: streamName,
                PartitionKey: partitionKey,
                Data: Buffer.from(JSON.stringify(message), 'utf8')
            }))
            .then(() => {
                func();
            })
            .catch(() => func());
    }, 10000);

    func();
};

kinesisReady.then(() => {
    console.log('waiting 5s for initialization');

    setTimeout(async () => {
        const client = new KinesisClient({
            endpoint: 'http://localstack:4566',
            region: 'us-east-1',
            tls: false
        });
        
        const createStream = new CreateStreamCommand({ StreamName: 'test_stream', ShardCount: 1 });
        
        let result = await client.send(createStream);

        console.log('stream created');

        await waitForStream(client, 'test_stream');
        console.log('stream ready');

        sendMessage(client, "test_stream", "1", { test: "hello world!" });
    }, 5000);
});