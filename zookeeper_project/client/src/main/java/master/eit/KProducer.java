package master.eit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KProducer {

    public KafkaProducer producer;
    public Properties props;

    public KProducer() {
        this.props = new Properties();
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public String sendMessage(int sendMessageCount, String direction, String sender, String topic, String msg) throws Exception {
        String message = "";
        Long time = System.currentTimeMillis();

        try {
            for (Long index = time; index < time + sendMessageCount; index++) {
                String key = direction +"=" + sender;
                ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, key,   ":" + msg);

                RecordMetadata metadata = (RecordMetadata) producer.send(record).get();

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
