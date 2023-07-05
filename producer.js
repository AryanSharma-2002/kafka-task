const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "myApp",
  brokers: ["127.0.0.1:9092"],
});

const produceRequests = async () => {
  try {
    const producer = kafka.producer();

    await producer.connect();

    const requests = [
      {
        url: "http://localhost:5000/api/requests",
        payload: { data: "request1" },
      },
      {
        url: "http://localhost:5000/api/requests",
        payload: { data: "request2" },
      },
      {
        url: "http://localhost:5000/api/requests",
        payload: { data: "request3" },
      },
      {
        url: "http://localhost:5000/api/requests",
        payload: { data: "request4" },
      },
    ];
    console.log("Connected to producer");

    for (const request of requests) {
      await producer.send({
        topic: "someReq",
        messages: [{ value: JSON.stringify(request) }],
      });
    }

    console.log("All requests sent");
    await producer.disconnect();
  } catch (error) {
    console.error("Error sending messages:", error);
  }
};

module.exports = { produceRequests };
