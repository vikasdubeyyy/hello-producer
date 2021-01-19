package in.flyspark.producer.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWIthoutKey {
	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		Properties p = new Properties();
		p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducer<String, String> firstProducer = new KafkaProducer<String, String>(p);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("myFirst", "Hie Kafka");
		firstProducer.send(record, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception ex) {
				Logger logger = LoggerFactory.getLogger(ProducerWIthoutKey.class);
				if (ex == null) {
					logger.info("Successfully received the details as: \n" + "Topic:" + metadata.topic() + "\n"
							+ "Partition:" + metadata.partition() + "\n" + "Offset" + metadata.offset() + "\n"
							+ "Timestamp" + metadata.timestamp());
				}

				else {
					logger.error("Can't produce,getting error", ex);

				}
			}
		});
		firstProducer.flush();
		firstProducer.close();
	}
}
