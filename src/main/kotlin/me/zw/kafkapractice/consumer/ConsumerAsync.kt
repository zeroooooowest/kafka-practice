package me.zw.kafkapractice.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*

/**
 * 비동기 가져오기
 * : commitAsync()는 commitSync()와 다르게 오프셋 커밋을 실패하더라도 재시도하지 않는다.
 */
fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "zw-kafka01:9092,zw-kafka02:9092,zw-kafka03:9092")
        put("group.id", "zw-consumer01")
        put("enable.auto.commit", "false")
        put("auto.offset.reset", "latest")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    }
    val consumer = KafkaConsumer<String, String>(props).apply {
        subscribe(listOf("zw-basic01"))
    }

    try {
        while (true) {
            val records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS))
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