const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors());

const { produceRequests } = require("./producer");
const { consumeRequests } = require("./consumer");

app.get("/api/requests", async (req, res) => {
  const randomNumber = Math.random();
  if (randomNumber <= 0.5) {
    res.status(200).json({ randNo: randomNumber, status: "success" });
    return;
  }
  res.status(400).json({ randNo: randomNumber, status: "failure" });
  return;
});

const kafkaWork = async () => {
  try {
    await produceRequests();
    console.log("\n\n");
    await consumeRequests();
  } catch (err) {
    console.log("error while working with kafka", err.message);
  }
};
app.get("/api/kafka", async (req, res) => {
  kafkaWork();
  res.status(200).send("called");
});
app.listen(5000, () => {
  console.log("listening at 5000");
});
