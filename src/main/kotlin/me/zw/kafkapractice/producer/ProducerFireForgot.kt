package me.zw.kafkapractice.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

// 메시지를 보내고 확인하지 않기
fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "zw-kafka01:9092,zw-kafka02:9092,zw-kafka03:9092")
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }

    val producer = KafkaProducer<String, String>(props)

    try {
        for (i in 0 until 3) {
            val record = ProducerRecord<String, String>("zw-basic01", "Apache Kafka Hello World - $i")
            producer.send(record)
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        producer.close()
    }
}