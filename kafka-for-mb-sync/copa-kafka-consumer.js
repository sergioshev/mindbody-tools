var kafka = require('kafka-node')

const kafkaConsumerDefaultOptions = {
  kafkaHost: process.env.KAFKA_HOST ||'kafka:9092'
}

const DEFAULT_TOPIC = process.env.KAFKA_FMS_TOPIC || 'FMS_WEB_HOOK'
const DEFAULT_PARTITION = process.env.KAFKA_FMS_PARTITION || 0
const DEFAULT_OFFSET = process.env.KAFKA_FMS_OFFSET || 0
//const SERVICE_NAME = process.env.SERVICE_NAME || 'fms'


evaluator = (m) => new Promise((resolve, reject) => {
  setTimeout(() => {
    if (Number(m) !== NaN) {
      if (!(m % 2)) return resolve(true)
    }

    reject('message failed')
  }, Math.floor(Math.random() * 1000))
})

const getBiggestConsecutiveFinishedOffset = (finishedOffsets, waitForOffset) => {
  finishedOffsets.sort( (a, b) => a-b)
  if (finishedOffsets[0] > waitForOffset) throw 'waitForOffset inexistent'

  return finishedOffsets.reduce(
    (acc, cur) => { if (acc + 1 === cur) { return cur } return acc }
  )
}

const kafkaTopicReader = (
  messageProcessor,
  { topic = DEFAULT_TOPIC, partition = DEFAULT_PARTITION, offset = DEFAULT_OFFSET } = {}
) => {
  var client = new kafka.KafkaClient(kafkaConsumerDefaultOptions)
  var consumer = new kafka.Consumer(
    client,
    [{ topic, partition, offset }],
    { autoCommit: false, fromOffset: true }  
  )

  var finishedOffsets = []
  var waitForOffset = null

  consumer.on('error', console.error)

  consumer.on('message', async function (payload) {
    const message = payload.value
    const currentOffset = payload.offset

    if (waitForOffset === null) { waitForOffset = currentOffset }

    console.log(`message ${message} at offset ${currentOffset}`)
   
    try {
      await messageProcessor(message)
    } catch (err) {
      console.error(`publish ${message} to topic ${topic}_invalid`)
    }
    
    finishedOffsets.push(currentOffset)
    console.log({ waitForOffset, finishedOffsets })
    try {
      const biggestFinishedOffset = getBiggestConsecutiveFinishedOffset(finishedOffsets, waitForOffset)
      consumer.setOffset(payload.partition, payload.topic, biggestFinishedOffset)
      finishedOffsets = finishedOffsets.filter(fo => fo > biggestFinishedOffset)
      waitForOffset = biggestFinishedOffset + 1

      console.log(`setted offset ${biggestFinishedOffset}`)
    } catch (err) {
      console.log(`skipped offset ${currentOffset}`)
    } 
  })

  return () => {
    consumer.close(true, function (err, message) {
      console.log("consumer has been closed", { message, err });
    })
  }
}

const kafkaReaderShutdown = kafkaTopicReader(evaluator)

setTimeout(() => {
  kafkaReaderShutdown()
}, 20 * 1000)