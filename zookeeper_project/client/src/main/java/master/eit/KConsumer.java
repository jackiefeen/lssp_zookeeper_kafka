package master.eit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KConsumer implements Runnable{

    private Consumer<String, String> consumer;
    private Properties props;
    private TopicPartition topicPartition;
    private ConsumerRecords<String, String> messages;

    public KConsumer(String topic, Integer partition) {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer"+partition);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.messages = ConsumerRecords.empty();
        this.topicPartition = new TopicPartition(topic, partition);
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        takeHistory();
    }

    private void takeHistory() {
        System.out.println("Reading");
        int giveUp = 100;
        int noRecordsCount = 0;

        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        this.messages = ConsumerRecords.empty();

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(5);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            } else {
                for (ConsumerRecord record : consumerRecords) {
                    System.out.println("Message: [T=" + record.topic() + "] " +
                                                "[P=" + record.partition() + "] " +
                                                "[O=" + record.offset() + "] " +
                                                "[Timestamp=" + record.timestamp() + "] " +
                                                "[K=" + record.key() + "] " +
                                                "[V=" + record.value() + "]");
                    //this.messages.add(record.key().toString()+record.value() + "\n");
                }

                this.messages = consumerRecords;
                break;
            }
        }

        consumer.close();
    }

    public ConsumerRecords<String, String> getMessages() {
        return this.messages;
    }
}
