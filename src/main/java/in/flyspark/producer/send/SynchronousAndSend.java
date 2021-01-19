package in.flyspark.producer.send;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class SynchronousAndSend {

	public static void main(String[] args) {
		try {
			Properties p = new Properties();
			p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			Producer<String, String> producer = new KafkaProducer<String, String>(p);
			ProducerRecord<String, String> pr = new ProducerRecord<String, String>("test-topic", "key-v", "vikas");
			RecordMetadata rm = producer.send(pr).get();
			System.out.println("Partition : " + rm.partition() + " | " + " Offset : " + rm.offset());
			producer.close();
			System.out.println("SynchronousAndSend");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
