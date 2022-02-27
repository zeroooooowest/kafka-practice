package me.zw.kafkapractice.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.lang.Exception

class ZwProducerCallback(
    private val record: ProducerRecord<String, String>,
) : Callback {

    override fun onCompletion(metadata: RecordMetadata, exception: Exception?) {
        if (exception != null) {
            exception.printStackTrace()
        } else {
            println("Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, Offset: ${metadata.offset()}, Key: ${record.key()}, Received Message: ${record.value()}")
        }
    }
}