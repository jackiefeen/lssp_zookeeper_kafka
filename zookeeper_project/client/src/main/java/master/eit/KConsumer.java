package master.eit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KConsumer {

    private Consumer<Long, String> consumer;
    private Properties props;
    private TopicPartition topicPartition;

    public KConsumer(String topic) {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.topicPartition = new TopicPartition(topic, 0);
        this.consumer = new KafkaConsumer<>(props);
    }

    public List<String> readMessage() {
        int giveUp = 100;
        int noRecordsCount = 0;

        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        List<String> messages = new ArrayList<>();
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(0);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord record: consumerRecords) {
                messages.add(record.value()+", T_"+record.key()+", P_"+record.partition()+", O_"+record.offset()+"\n");
            }
        }

        consumer.close();
        System.out.println("Done");
        return messages;
    }
}
