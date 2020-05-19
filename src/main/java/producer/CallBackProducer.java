package producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallBackProducer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "datanode101:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 配置分区器 Partitioner
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "partitioner.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("third-topic", Integer.toString(i)), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "----" + metadata.offset());
                    }
                }
            });
        }

        producer.close();
    }
}
