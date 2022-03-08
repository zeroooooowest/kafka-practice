package me.zw.kafkapractice.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*


fun main() {
    val props = Properties().apply {
        setProperty(BOOTSTRAP_SERVERS_CONFIG, "zw-kafka01:9092,zw-kafka02:9092,zw-kafka03:9092")
        setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true")
        setProperty(ACKS_CONFIG, "all")
        setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
        setProperty(RETRIES_CONFIG, "5")
        setProperty(TRANSACTIONAL_ID_CONFIG, "zw-transaction-01")
    }

    val producer = KafkaProducer<String, String>(props)

    producer.initTransactions()
    producer.beginTransaction()
    try {
        for (i in 0 until 1) {
            val record = ProducerRecord<String, String>("zw-test05", "Apache Kafka Exactly Once - $i")
            producer.send(record)
            producer.flush()
            println("Message sent successfully")
        }
    } catch (e: Exception) {
        producer.abortTransaction()
        e.printStackTrace()
    } finally {
        producer.commitTransaction()
        producer.close()
    }
}