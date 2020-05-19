package producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Properties;

public class TopicUtils {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "datanode101:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        AdminClient client = AdminClient.create(properties);
        ArrayList<NewTopic> topics = new ArrayList<NewTopic>();
        topics.add(new NewTopic("fourth-topic", 1, (short) 1));
        client.createTopics(topics);

        client.close();
    }
}
