const Kafka = require('node-rdkafka')
const logger = require('./utils/winston')

const consumer = new Kafka.KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
  'offset_commit_cb': (err, topicPartitions) => {
    if (err) {
      logger.error(`[APACHE-KAFKA CONSUMER] ${err.message || JSON.stringify(err)}`)
    } else {
      logger.info(`[APACHE-KAFKA CONSUMER] ${topicPartitions.message || JSON.stringify(topicPartitions)}`)
    }
  }
})

// Menangani event 'ready' ketika konsumer berhasil terhubung
consumer.on('ready', (arg) => {
  logger.info(`[APACHE-KAFKA CONSUMER] Consumer has beed connected and ready. ${arg.message || JSON.stringify(arg)}`)

  // Berlangganan ke topik 'test-topic'
  // Ganti 'test-topic' dengan nama topik yang Anda inginkan
  consumer.subscribe(['test-topic'])

  // Memulai konsumsi pesan dari topik yang dilangganinya
  consumer.consume()
})

// Menangani event 'data' untuk setiap pesan yang diterima
consumer.on('data', (data) => {
  // Pesan yang diterima akan ada di properti 'value'
  // Pastikan untuk mengonversi Buffer ke string atau format yang Anda butuhkan
  console.log(data);

  logger.info(`[APACHE-KAFKA CONSUMER] Received: ${data.value.toString()}`)
})

// Menangani event 'event.error' untuk semua error yang terjadi
consumer.on('event.error', (err) => {
  logger.error(`[APACHE-KAFKA CONSUMER] ${err.message || JSON.stringify(err)}`)
})

// Menghubungkan konsumer ke broker
consumer.connect()

// Menangani sinyal untuk mematikan konsumer dengan rapi
process.on('SIGINT', () => {
  logger.info('[APACHE-KAFKA CONSUMER] Shut down..')
  consumer.disconnect()
})