package me.zw.kafkapractice.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*


fun main() {
    val props = Properties().apply {
        setProperty(BOOTSTRAP_SERVERS_CONFIG, "zw-kafka01:9092,zw-kafka02:9092,zw-kafka03:9092")
        setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        setProperty(GROUP_ID_CONFIG, "zw-consumer-01")
        setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
        setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")
        setProperty(ISOLATION_LEVEL_CONFIG, "read_committed")
    }

    val consumer = KafkaConsumer<String, String>(props).apply {
        subscribe(listOf("zw-test05"))
    }

    try {
        while (true) {
            val records = consumer.poll(Duration.ofMillis(1000))
            for (record in records) {
                println("Topic: ${record.topic()}, Partition: ${record.partition()}, Offset: ${record.offset()}, Key: ${record.key()}, Value: ${record.value()}")
            }
            consumer.commitAsync()
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        consumer.close()
    }
}