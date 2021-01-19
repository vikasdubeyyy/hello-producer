package in.flyspark.producer.basics;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWIthKey {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(ProducerWIthKey.class);
		String bootstrapServer = "127.0.0.1:9092";
		Properties p = new Properties();
		p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducer<String, String> firstProducer = new KafkaProducer<String, String>(p);
		for (int i = 0; i < 10; i++) {
			String topic = "myFirst";
			String key = "Key" + i;
			String value = "Value" + i;
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			logger.info("key : " + key);
			firstProducer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception ex) {
					if (ex == null) {
						logger.info("Successfully received the details as: \n" + "Topic:" + metadata.topic() + "\n"
								+ "Partition:" + metadata.partition() + "\n" + "Offset" + metadata.offset() + "\n"
								+ "Timestamp" + metadata.timestamp());
					} else {
						logger.error("Can't produce,getting error", ex);

					}
				}
			}).get();
		}
		firstProducer.flush();
		firstProducer.close();
	}
}
