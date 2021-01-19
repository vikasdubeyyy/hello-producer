package in.flyspark.producer.send;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class AsynchronousAndSend {

	public static void main(String[] args) {
		try {
			Properties p = new Properties();
			p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			Producer<String, String> producer = new KafkaProducer<String, String>(p);
			ProducerRecord<String, String> pr = new ProducerRecord<String, String>("test-topic", "key-v", "vikas");
			producer.send(pr, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception ex) {
					if (ex == null) {
						System.out.println("Successfully received the details as: \n" + "Topic:" + metadata.topic()
								+ "\n" + "Partition:" + metadata.partition() + "\n" + "Offset" + metadata.offset()
								+ "\n" + "Timestamp" + metadata.timestamp());
					} else {
						System.out.println("Can't produce,getting error" + ex);

					}
				}
			}).get();
			producer.close();
			System.out.println("AsynchronousAndSend");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
