package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class StringProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // Kafka集群地址
        properties.put("bootstrap.servers", "datanode101:9092");
        // ack应答级别
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 默认大小16K，发送给每个分区的记录批次大小，单位是字节；太小会减少吞吐量，太大则会增加内存开销。
        properties.put("batch.size", "16384");
        // 生产者会等待指定的时间以便将待发往同一个分区的记录添加到一个批次（batch）中。注意：如果发往同一个分区的记录大小达到了上面指定的batch.size的大小，会忽略该参数指定时间立即发送。
        // 该参数默认值是0，即没有延迟。
        properties.put("linger.ms", 2);

        // 生产者（KafkaProducer）包含一个缓冲区（RecordAccumulator）和一个后台I/O线程（Sender）。
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            // send()方法是异步地，它会将记录加入到缓冲区，然后立即返回。
            producer.send(new ProducerRecord<String, String>("third-topic", Integer.toString(i), Integer.toString(i)));
        }

//        Thread.sleep(100000000);
        producer.close();
    }
}
