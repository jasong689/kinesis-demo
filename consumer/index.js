const { KinesisClient, GetShardIteratorCommand, ListShardsCommand, DescribeStreamCommand, RegisterStreamConsumerCommand, GetRecordsCommand } = require("@aws-sdk/client-kinesis");
const axios = require('axios');

console.log('consumer started');

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

const getRecords = async (client, iterator) => {
    return client.send(new GetRecordsCommand({
        ShardIterator: iterator
    }));
};

const sleep = async (ms) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => resolve(), ms);
    });
}

kinesisReady.then(() => {
    console.log('waiting 5s for initialization');

    setTimeout(async () => {
        const client = new KinesisClient({
            endpoint: 'http://localstack:4566',
            region: 'us-east-1',
            tls: false
        });

        let streamArn = await waitForStream(client, 'test_stream');
        console.log('stream ready');

        console.log('registering consumer');
        let { Consumer: { ConsumerARN } } = await client.send(new RegisterStreamConsumerCommand({ ConsumerName: "consumer", StreamARN: streamArn }));
        
        let listShardsCmd = new ListShardsCommand({ StreamName: 'test_stream' });

        let { Shards: [{ ShardId }] } = await client.send(listShardsCmd);

        console.log(`iterator for shardId: ${ShardId}, consumerArn: ${ConsumerARN}`);
        let { ShardIterator } = await client.send(new GetShardIteratorCommand({
            StreamName: 'test_stream',
            ShardId: ShardId,
            ShardIteratorType: 'TRIM_HORIZON'
        }));

        while(true) {
            let { Records, NextShardIterator } = await getRecords(client, ShardIterator);

            if (Records.length > 0) {
                Records.forEach(r => {
                    let buf = Buffer.from(r.Data)
                    let str = buf.toString('utf-8');

                    console.log(JSON.parse(str));
                });
            }

            ShardIterator = NextShardIterator;

            await sleep(1000);
        }
    }, 5000);
});