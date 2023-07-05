const { Kafka } = require("kafkajs");
const axios = require("axios");

const kafka = new Kafka({
  clientId: "myApp",
  brokers: ["127.0.0.1:9092"],
});
const producer = kafka.producer();

const consumeRequests = async () => {
  try {
    const consumer = kafka.consumer({ groupId: "newRequestGroup" });

    await consumer.connect();
    await consumer.subscribe({ topic: "someReq", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        // console.log("Received message:", message.value.toString());

        try {
          const request = JSON.parse(message.value.toString());
          console.log("Consumed Message ", request.url, request?.payload?.data);
          const response = await axios.get(request.url);
          console.log("API response:", response.data);
          console.log("\n");
          await consumer.commitOffsets([
            { topic, partition, offset: message.offset },
          ]);
        } catch (error) {
          console.error("Error processing API request:\n");
          await producer.connect();

          await producer.send({
            topic: "someReq",
            messages: [{ value: message.value.toString() }],
          });
          await producer.disconnect();
        }
      },
    });
  } catch (error) {
    console.error("Error consuming messages:", error);
  }
};
// consumeRequests();
module.exports={consumeRequests};
