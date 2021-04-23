const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
  clientId: "hanulse-kafka",
  brokers: [
    "b-2.hanulse-kafka-clu.6yw7aq.c4.kafka.ap-northeast-2.amazonaws.com:9094",
    "b-1.hanulse-kafka-clu.6yw7aq.c4.kafka.ap-northeast-2.amazonaws.com:9094",
    "b-3.hanulse-kafka-clu.6yw7aq.c4.kafka.ap-northeast-2.amazonaws.com:9094"
  ],
  retry: {
    initialRetryTime: 100,
    retires: 8
  },
  ssl: true,
});

const producer = kafka.producer();
const consumer = kafka.consumer({groupId: 'test-' + Date.now()});
const admin = kafka.admin();

async function createTopic(topic, repFactor) {
  //await admin.connect();
  await admin.createTopics({
    validateOnly: false,
    waitForLeaders: true,
    timeout: 5000,
    topics: [
      {
        topic: topic,
        numPartitions: 1,     // default: 1
        replicationFactor: repFactor || 1, // default: 1
        //replicaAssignment: [{partition: 0, replicas: [0, 1, 2]}],  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
        //configEntries:<Array>       // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
      }
    ]
  });
  await admin.disconnect();
}

async function publish(topic, message) {
  await producer.connect();
  await producer.send({
    topic: topic,
    compression: CompressionTypes.GZIP,
    messages: [
      {value: message},
    ],
  });
}

async function subscribe(topic) {
  await consumer.connect();
  await consumer.subscribe({ topic: topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(new Date(), "topic: " + topic + ", partition: " + partition + ", offset: " + message.offset + ", message: " + message.value.toString())
    },
  });
}

module.exports = {
  kafka: kafka,
  admin: admin,
  producer: producer,
  consumer: consumer,
  createTopic: createTopic,
  publish: publish,
  subscribe: subscribe
};
