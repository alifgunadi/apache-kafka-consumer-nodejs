require('dotenv').config({ quiet: true })
const Kafka = require('node-rdkafka')
const logger = require('./utils/winston')

const consumer = new Kafka.KafkaConsumer({
  'group.id': 'consumer-12',
  'metadata.broker.list': 'localhost:9092',
  'fetch.message.max.bytes': 52428800,
  'max.partition.fetch.bytes': 52428800,
  'offset_commit_cb': (err, topicPartitions) => {
    if (err) {
      logger.error(`[KAFKA CONSUMER] ${err.message || JSON.stringify(err)}`)
    } else {
      logger.debug(`[KAFKA CONSUMER] ${topicPartitions.message || JSON.stringify(topicPartitions)}`)
    }
  }
}, {
  'auto.offset.reset': 'earliest',
})

let topic = process.env.DETAIL_TOPIC

consumer.on('ready', (arg) => {
  logger.info(`[KAFKA CONSUMER] Consumer has beed connected and ready. ${arg.message || JSON.stringify(arg)}`)
  consumer.subscribe([`${topic}`])
  consumer.consume()
})

consumer.on('data', (data) => {
  let message = data.value.toString()
  let topic = data.topic
  logger.debug(`[KAFKA CONSUMER] Topic: ${JSON.stringify(topic, null, 2)}. Received message: ${message}`)
})

consumer.on('event.error', (err) => {
  logger.error(`[KAFKA CONSUMER] ${err.message || JSON.stringify(err)}`)
})

consumer.connect()