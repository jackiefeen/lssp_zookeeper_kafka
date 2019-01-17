package master.eit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * KAFKA CONSUMER CLASS
 */
public class KConsumer implements Runnable{

    // Variables
    private Consumer<String, String> consumer;
    private Properties props;
    private TopicPartition topicPartition;
    private ConsumerRecords<String, String> messages;

    /**
     * CONSTRUCTOR
     *
     * @param topic It is the Kafka topic where the messages to read are contained
     * @param partition It is the partition in which the consumer must read
     */
    public KConsumer(String topic, Integer partition) {
        // Setting the properties for the consumer
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer"+partition);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // This settings allow to get all the history
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Instantiating working objects
        this.messages = ConsumerRecords.empty();
        this.topicPartition = new TopicPartition(topic, partition);
        this.consumer = new KafkaConsumer<>(props);
    }

    // Overriding the run function
    @Override
    public void run() {
        takeHistory();
    }

    /**
     * takeHistory Method
     *
     * Retrieve all the messages from Kafka (the History of a given user)
     */
    private void takeHistory() {
        System.out.println("Reading");

        // Set the limits for the maximum number of requests made to Kafka
        int giveUp = 100;
        int noRecordsCount = 0;

        // SForce the consumer to read at the beginning of the partition
        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        this.messages = ConsumerRecords.empty();

        // Loop until something is read BUT at maximum 100 times
        while (true) {
            // Ask to Kafka for messages, wait 5 ms for the answer
            ConsumerRecords<String, String> consumerRecords = consumer.poll(5);

            // IF there are no records THEN try it again BUT exit the while loop if you are trying more the 100 times
            // ELSE there are records! So exit the while loop and close the consumer
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
                }

                this.messages = consumerRecords;
                break;
            }
        }

        consumer.close();
    }

    // Getter for the variable containing messages
    public ConsumerRecords<String, String> getMessages() {
        return this.messages;
    }
}
