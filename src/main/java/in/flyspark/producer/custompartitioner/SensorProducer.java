package in.flyspark.producer.custompartitioner;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class SensorProducer {

	public static void main(String[] args) {
		try {
			Properties p = new Properties();
			p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			p.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, SensorPartitioner.class.getName());
			p.setProperty("speed.sensor.name", "TSS");

			String topicName = "SensorTopic";

			Producer<String, String> producer = new KafkaProducer<String, String>(p);
			for (int i = 0; i < 10; i++)
				producer.send(new ProducerRecord<String, String>(topicName, "SSP" + i, "500" + i));

			for (int i = 0; i < 10; i++)
				producer.send(new ProducerRecord<String, String>(topicName, "TSS", "500" + i));

			producer.close();
			System.out.println("SynchronousAndSend");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
