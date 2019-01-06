package master.eit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KConsumer {

    private Consumer<Long, String> consumer;
    private Properties props;

    public KConsumer(String topic) {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("Gioele"));
    }

    public void readMessage() {
        int giveUp = 100;
        int noRecordsCount = 0;

        consumer.seekToBeginning(consumer.assignment());
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(0);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord record: consumerRecords) {
                System.out.println("KConsumer Record:("+record.key()+", "+record.value()+", "+record.partition()+", "+record.offset()+")\n");
                
            }
        }

        consumer.close();
        System.out.println("Done");
    }
}
