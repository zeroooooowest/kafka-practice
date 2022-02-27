package me.zw.kafkapractice.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

// 동기 전송
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

            // send 메서드가 Future 를 리턴하고 get 메서드를 이용해 기다린다.
            val metadata = producer.send(record).get()
            println("Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, Offset: ${metadata.offset()}, Key: ${record.key()}, Received Message: ${record.value()}")
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        producer.close()
    }
}