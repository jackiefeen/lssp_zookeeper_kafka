package master.eit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * REFRESH CLASS
 *
 * This class implements Runnable and can be used as a Thread.
 * When started, It acts as a Main Thread able to manage a given
 * of Children (the KConsumers).
 *
 * When the Consumers (children) terminate with results, this class
 * ensures that all of them are terminated before refreshing the GUI.
 *
 * Unless It is interrupted, this Thread works infinitely running the
 * job above every 5 seconds.
 */
public class Refresher implements Runnable {

    private String topic;
    private Integer parallelism;

    public Refresher(String topic, Integer parallelism) {
        this.topic = topic;
        this.parallelism = parallelism;
    }

    @Override
    public void run() {
        // Instantiazing working Lists
        // consumerList : list of consumers needed
        // threadsList : number of threads (one for each consumer)
        // dead : flag to label if a thread is dead or alive
        List<KConsumer> consumersList = new ArrayList<>();
        List<Thread> threadsList = new ArrayList<>();
        List<Boolean> dead = new ArrayList<>();
        try {
            while (true) {
                // Cleaning Lists before proceeding
                consumersList.clear();
                threadsList.clear();
                dead.clear();

                // Creating Required Consumers
                // Creating Threads
                // Updating Threads' flags
                // Starting Consumers
                for (int i = 0; i < parallelism; i++) {
                    consumersList.add(new KConsumer(topic, i));
                    threadsList.add(new Thread(consumersList.get(i)));
                    dead.add(false);
                    threadsList.get(i).start();
                }

                // Waiting that all the threads die (all the messages from all the partitions have been retrieved)
                // The result is a list of list since each customer will retrieve one partition
                List<ConsumerRecords<String,String>> messages = new ArrayList<>();
                while (dead.contains(false)) {
                    for (int i = 0; i < parallelism; i++) {
                        if (!threadsList.get(i).isAlive() && !dead.get(i)) {
                            System.out.println("Consumer " + i + " properly ended");
                            dead.set(i, true);
                            messages.add(consumersList.get(i).getMessages());
                        }
                    }
                }

                // Making the List of List a flat list
                List<ConsumerRecord> flat = new ArrayList<>();
                for (ConsumerRecords<String,String> list:messages) {
                    for (ConsumerRecord record: list) {
                        flat.add(record);
                    }
                }

                // Then, if consumers received at least one message to show go through the if below
                if (!flat.isEmpty()) {
                    // Re-ordering retrieved messages based on Timestamp
                    Collections.sort(flat, new Comparator<ConsumerRecord>() {
                        public int compare(ConsumerRecord o1, ConsumerRecord o2) {
                            return new Long(o1.timestamp()).compareTo(new Long (o2.timestamp()));
                        }
                    });

                    // Refresh the GUI
                    Client.form.textAreaMsg.setText("");
                    for (ConsumerRecord record : flat) {
                        String msg = record.key().toString()+record.value() + "\n";
                        try {
                            if (msg.contains("=" + Client.form.listOnline.getSelectedValue().toString().split(" ")[0])) {
                                if (msg.substring(0, 1).equals("S"))
                                    Client.form.textAreaMsg.append("You say: " + msg.split(":")[1]);
                                else {
                                    String sender = msg.split("=")[1].split(":")[0];
                                    String message = msg.split("=")[1].split(":")[1];
                                    Client.form.textAreaMsg.append(sender + " says: " + message);
                                }
                            }
                        } catch (NullPointerException e) {
                            String sender = msg.split("=")[1].split(":")[0];
                            String message = msg.split("=")[1].split(":")[1];
                            if (sender.equals(Client.form.functionText.getText()))
                                Client.form.textAreaMsg.append("You say: " + message);
                            else
                                Client.form.textAreaMsg.append(sender + " says: " + message);
                        }
                    }
                }

                // Wait 5 seconds before refreshing
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            // Ensure that all the children are dead before proceeding
            while (dead.contains(false)) {
                for (int i = 0; i < parallelism; i++) {
                    if (!threadsList.get(i).isAlive() && !dead.get(i)) {
                        System.out.println("Ending Consumer " + i);
                        dead.set(i, true);
                    }
                }
            }
            System.out.println("Main Thread Closed");
        }
    }
}
