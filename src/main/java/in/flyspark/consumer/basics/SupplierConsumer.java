package in.flyspark.consumer.basics;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import in.flyspark.producer.customserializer.Supplier;
import in.flyspark.producer.customserializer.SupplierDeserializer;

public class SupplierConsumer {

	public static void main(String[] args) throws Exception {

		String topicName = "SupplierTopic";
		String groupName = "SupplierTopicGroup";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("group.id", groupName);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", SupplierDeserializer.class.getName());

		KafkaConsumer<String, Supplier> consumer = null;

		try {
			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList(topicName));

			while (true) {
				ConsumerRecords<String, Supplier> records = consumer.poll(100);
				for (ConsumerRecord<String, Supplier> record : records) {
					System.out.println("Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = "
							+ record.value().getName() + " Supplier Start Date = "
							+ record.value().getStartDate().toString());
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
