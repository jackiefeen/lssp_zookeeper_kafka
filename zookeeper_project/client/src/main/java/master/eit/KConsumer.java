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
    public static List<String> messages = new ArrayList<>();

    public KConsumer(String topic) {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.topicPartition = new TopicPartition(topic, 0);
        this.consumer = new KafkaConsumer<>(props);
    }

    public void setTopic (String topic) {
        this.topicPartition = new TopicPartition(topic, 0);
    }

    public void takeHistory() {
        System.out.println("Reading");
        int giveUp = 100;
        int noRecordsCount = 0;

        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        messages.clear();
        do {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(0);

                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) break;
                    else continue;
                }

                for (ConsumerRecord record : consumerRecords) {
                    System.out.println("I have got something:" + record);
                    //messages.add(record.value()+", T_"+record.key()+", P_"+record.partition()+", O_"+record.offset()+"\n");
                    messages.add(record.value() + "\n");
                }
            }
        } while (messages.isEmpty());

        Client.form.textArea1.setText("");
        for (String msg:KConsumer.messages) {
            try {
                if (msg.contains(Client.form.functionText.getText()+"="+Client.form.listOnline.getSelectedValue().toString().split(" ")[0])) {
                    if (msg.substring(0, 1).equals("S"))
                        Client.form.textArea1.append("You say: " + msg.split(":")[1]);
                    else {
                        String sender = msg.split("=")[1].split(":")[0];
                        String message = msg.split("=")[1].split(":")[1];
                        Client.form.textArea1.append(sender+" says: "+message);
                    }
                }
            } catch (NullPointerException e) {
                if (msg.contains("-chatroom-"+Client.form.chatUserLabel.getText())) {
                    if (msg.substring(0, 1).equals("S"))
                        Client.form.textArea1.append("You say: " + msg.split(":")[1]);
                    else {
                        String sender = msg.split("=")[1].split(":")[0];
                        String message = msg.split("=")[1].split(":")[1];
                        if (sender.equals(Client.form.functionText.getText()))
                            Client.form.textArea1.append("You say: "+message);
                        else
                            Client.form.textArea1.append(sender+" says: "+message);
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                takeHistory();
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            System.out.println("Thread Interrupted");
        }
    }
}
