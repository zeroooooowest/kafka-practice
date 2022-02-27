package me.zw.kafkapractice.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*

/**
 * 오토 커밋
 * : 오프셋을 주기적으로 커밋하므로 관리자가 따로 오프셋을 관리 안해도 된다는 장점이 있지만,
 *  컨슈머 종료 등이 빈번히 일어나면, 일부 메시지를 못 가져오거나 중복으로 가져오는 경우가 존재한다.
 *
 */
fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "zw-kafka01:9092,zw-kafka02:9092,zw-kafka03:9092")
        put("group.id", "zw-consumer01")
        put("enable.auto.commit", "true")
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
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        consumer.close()
    }
}