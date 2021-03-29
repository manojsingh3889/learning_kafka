import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration

@Slf4j
class ConsumerTest {

    static void main(String[] args) {
        Properties properties = new Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.32:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.name)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "trace_group_1")
        properties.setProperty(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO")
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10000")
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000")
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "20000")
        properties.setProperty('schema.registry.url', 'http://localhost:8101/')

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties)

        consumer.subscribe(['local.plip-it-ds.policy_trace'])

        while (true) {
            def results = consumer.poll(Duration.ofMillis(1000l))

            results.forEach { result ->
                print(result.value())
            }
        }
    }

    static void print(GenericRecord record) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema())
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos)
        writer.write(record, encoder)
        encoder.flush()
        baos.flush()
        println JsonOutput.prettyPrint(new String(baos.toByteArray()))
    }
}
