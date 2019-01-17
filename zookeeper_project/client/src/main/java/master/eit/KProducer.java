package master.eit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * KAFKA PRODUCER CLASS
 *
 * The class that permits to write messages into a topic
 */
public class KProducer {

    // Variables
    public KafkaProducer producer;
    public Properties props;

    /**
     * CONSTRUCTOR
     */
    public KProducer() {
        // Properties of the producer
        this.props = new Properties();
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Instantiate Producer
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * SendMessage Method
     *
     * @param sendMessageCount number of messages to send
     * @param direction It can be R (Received) or S (Sent)
     * @param sender The sender of the message
     * @param topic The topic in which the message must be sent
     * @param msg The message to send
     * @return The message written written
     * @throws Exception
     */
    public String sendMessage(int sendMessageCount, String direction, String sender, String topic, String msg) throws Exception {
        String message = "";
        Long time = System.currentTimeMillis();

        try {
            for (Long index = time; index < time + sendMessageCount; index++) {
                String key = direction +"=" + sender;
                // Record to send
                ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, key,   ":" + msg);

                // Send the record to the topic
                RecordMetadata metadata = (RecordMetadata) producer.send(record).get();

                // Display Information about the message sent
                Long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +  "meta(partition=%d, offset=%d) time=%d\n",
                                  record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                System.out.println("I have sent to: " + record.topic() + "...."+ record.value());
                //message = "You says: " + msg + ", T_"  + record.key() + ", P_" + metadata.partition() + ", O_" + metadata.offset();
                message = "You say: " + msg;
            }
        } finally {
            producer.flush();
            producer.close();
        }

        return message;
    }

}
