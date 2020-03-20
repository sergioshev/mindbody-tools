var kafka = require('kafka-node')

const kafkaConsumerDefaultOptions = {
  kafkaHost: process.env.KAFKA_HOST ||'kafka:9092'
}

const DEFAULT_TOPIC = process.env.KAFKA_FMS_TOPIC || 'FMS_WEB_HOOK'
const DEFAULT_PARTITION = process.env.KAFKA_FMS_PARTITION || 0
const DEFAULT_OFFSET = process.env.KAFKA_FMS_OFFSET || 0
//const SERVICE_NAME = process.env.SERVICE_NAME || 'fms'

evaluator = (m) => {
  if (Number(m) !== NaN) {
    if (!(m % 2)) return true
  }

  throw Error('message failed')
}

const kafkaTopicReader = (
  messageProcessor,
  { topic = DEFAULT_TOPIC, partition = DEFAULT_PARTITION, offset = DEFAULT_OFFSET } = {}
) => {
  var client = new kafka.KafkaClient(kafkaConsumerDefaultOptions)
  
  var consumer = new kafka.ConsumerStream(
    client,
    [{ topic, partition, offset }],
    {
      autoCommit: false,
      fetchMaxWaitMs: 100,
      fetchMinBytes: 1,
      fetchMaxBytes: 1024 * 1024
    }
  )

  consumer.on('error', console.error)

  consumer.on('data', function (payload) {
    const message = payload && payload.value
    console.log({message})
  
    { 
      (async () => {
        try {
          await messageProcessor(message)
        } catch (err) {
          console.error(`publish ${message} to topic ${topic}_invalid`)
        }
      })()
    }
    console.log({ offset: payload.offset + 1 })
  })

  return () => {
    consumer.close(function (err, message) {
      console.log("consumer has been closed", { message, err });
    })
  }
}

const kafkaReaderShutdown = kafkaTopicReader(evaluator)

setTimeout(() => {
  kafkaReaderShutdown()
}, 10 * 1000)