var kafka = require('kafka-node')

const KAFKA_HOST = process.env.KAFKA_HOST || 'kafka:9092'
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'FMS_WEB_HOOK'
const KAFKA_PARTITION = process.env.KAFKA_PARTITION || 0
const KAFKA_OFFSET = process.env.KAFKA_OFFSET || 0
const KAFKA_FROM_BEGINNING = (
  process.env.KAFKA_FROM_BEGINNING === 'true' ||
  process.env.KAFKA_FROM_BEGINNING === true || true
)

const kafkaClientDefaultOptions = {
  kafkaHost: KAFKA_HOST
}

const kafkaConsumerDefaultOptions = {
  autoCommit: false,
  fromOffset: KAFKA_FROM_BEGINNING
}

const kafkaDefaultTopic = {
  topic: KAFKA_TOPIC,
  partition: KAFKA_PARTITION,
  offset: KAFKA_OFFSET
}

const readKafkaTopic = async (messageProcessor = defaultMessageProcessor) => {
  var finishedOffsets = []
  var waitForOffset = null
  const { consumer, producer } = await getSafeConsumerAndProducer()

  const onMessageHandler = onMessageHandlerBuilder({
    consumer, producer, finishedOffsets, waitForOffset, messageProcessor
  })

  consumer.on('error', onErrorHandler)
  consumer.on('message', onMessageHandler)

  return () => {
    consumer.close(true, function (err, message) {
      console.log("consumer has been closed", { message, err });
    })
    producer.close(function (err, message) {
      console.log("producer has been closed", { message, err });
    })
  }
}

const getSafeConsumerAndProducer = () => new Promise(
  (resolve, reject) => {
    let consumer
    const producer = getKafkaProducer()

    createTopics(producer, reject)

    setTimeout(() => {
      consumer = getKafkaConsumer()
      resolve({ consumer, producer })
    }, 500)
  }
)

const createTopics = (producer, onError) => {
  const commonTopicOptions = { partitions: 1, replicationFactor: 1 }
  const topics = [
    { topic: kafkaDefaultTopic.topic, ...commonTopicOptions },
    { topic: `${kafkaDefaultTopic.topic}_INVALID`, ...commonTopicOptions }
  ]

  producer.on('error', onError)
  producer.on('ready', () => {
    producer.createTopics(topics, (err, result) => {
      if (err) {
        onError(err)
      }
    })
  })
}

const defaultMessageProcessor = () => Promise.resolve(true)

const onErrorHandler = (...args) => console.error(...args)

const onMessageHandlerBuilder = ({
  consumer,
  producer,
  waitForOffset,
  finishedOffsets,
  messageProcessor
}) => async (payload) => {
  const {
    value: message,
    offset: currentOffset,
    topic,
    partition
  } = payload

  if (waitForOffset === null) { waitForOffset = currentOffset }

  console.log(`message ${message} at offset ${currentOffset}`)

  try {
    await messageProcessor(message)
  } catch (err) {
    try {
      await sendToInvalidTopic(producer, message)
    } catch (errFromInvalid) {
      throw `Failure sending to invalid topic: ${errFromInvalid}`
    }
  }

  finishedOffsets.push(currentOffset)

  console.log({ waitForOffset, finishedOffsets: finishedOffsets.length })
  try {
    const biggestFinishedOffset = getBiggestConsecutiveFinishedOffset(finishedOffsets, waitForOffset)
    finishedOffsets = removeFinishedBlock(finishedOffsets, biggestFinishedOffset)
    waitForOffset = biggestFinishedOffset + 1

    consumer.setOffset(partition, topic, waitForOffset)
    console.log(`setted offset ${biggestFinishedOffset}`)
  } catch (err) {
    console.log(`skipped offset ${currentOffset}`)
  }
}

const sendToInvalidTopic = (producer, message) => {
  const payload = {
    topic: `${kafkaDefaultTopic.topic}_INVALID`,
    messages: message
  }

  return new Promise((resolve, reject) => {
    producer.send([payload], (err, data) => {
      if (err) {
        return reject(err)
      }
      console.log({payload, data})
      return resolve(data)
    })
  })
}

const getBiggestConsecutiveFinishedOffset = (finishedOffsets, waitForOffset) => {
  finishedOffsets.sort((a, b) => a - b)
  if (finishedOffsets[0] > waitForOffset) throw 'waitForOffset inexistent'

  return finishedOffsets.reduce(
    (acc, cur) => { if (acc + 1 === cur) { return cur } return acc }
  )
}

const removeFinishedBlock = (finishedOffsets, biggestFinishedOffset) => finishedOffsets
  .filter(fo => fo > biggestFinishedOffset)

const getKafkaConsumer = (clientOptions = {}, consumerOptions = {}) => {
  var client = getKafkaClient(clientOptions)

  return new kafka.Consumer(
    client,
    [kafkaDefaultTopic],
    {
      ...kafkaConsumerDefaultOptions,
      ...consumerOptions
    }
  )
}

const getKafkaProducer = (clientOptions = {}, producerOptions = {}) => {
  var client = getKafkaClient(clientOptions)

  return new kafka.Producer(client, producerOptions)
}

const getKafkaClient = (clientOptions = {}) => {
  return new kafka.KafkaClient({
    ...kafkaClientDefaultOptions,
    ...clientOptions
  })
}

evaluator = (m) => new Promise((resolve, reject) => {
  setTimeout(() => {
    if (Number(m) !== NaN) {
      if (!(m % 2)) return resolve(true)
    }

    reject('message failed')
  }, Math.floor(Math.random() * 1000))
})

let kafkaReaderShutdown 

(async () => kafkaReaderShutdown = await readKafkaTopic(evaluator))()

setTimeout(() => {
  kafkaReaderShutdown()
}, 60 * 1000)