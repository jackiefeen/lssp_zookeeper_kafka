package master.eit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;



public class Manager implements Runnable {
    private static final Logger logger = LogManager.getLogger("Manager Class");
    private static ZooKeeper zkeeper;
    private static String enrollpath = "/request/enroll";
    private static String quitpath = "/request/quit";
    private static String onlinepath = "/online";
    private static String registrypath = "/registry";
    private static final String[] TreeStructure = {"/request", "/request/enroll", "/request/quit", "/registry", "/online"};
    public boolean alive = true;
    private Watcher enrollbranchWatcher;
    private Watcher quitbranchWatcher;
    private Watcher onlinebranchWatcher;
    private List<String> registeredUsers;


    //constructor of the Manager
    public Manager(String hostPort) throws IOException, InterruptedException, KeeperException {

        //setup connection with zookeeper
        zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State: " + zkeeper.getState());

        if (zkeeper != null) {
            //ensure that the enrollpath actually exists before monitoring it, if not, create the tree structure
            try {
                if ((zkeeper.exists(enrollpath, false) == null) ||
                        (zkeeper.exists(quitpath, false) == null) ||
                        (zkeeper.exists(onlinepath, false) == null) ||
                        (zkeeper.exists(registrypath, false) == null)) {
                    createZkTreeStructure();
                } else {
                    logger.info("The Tree Structure exists already.");
                }
            } catch (KeeperException e) {
                e.printStackTrace();
            }

        } else {
            logger.warn("There is an issue with the ZooKeeper connection");
        }

        //get the status of registered users from /registry
        registeredUsers = getRegisteredUsers();
        logger.info("System State: registered users: " + registeredUsers);

        //initialize the ChildWatcher for enroll
        ChildWatcher enrollbranchWatcher = new ChildWatcher(this);
        Thread enrollbranchWatcherthread = new Thread();
        enrollbranchWatcherthread.start();
        this.enrollbranchWatcher = enrollbranchWatcher;
        List<String> enrollchildren = zkeeper.getChildren(enrollpath, this.enrollbranchWatcher, null);

        //if requests arrive before the manager starts up
        if (!enrollchildren.isEmpty()) {
            registerUser();
        }


        //initialize the ChildWatcher for quit
        ChildWatcher quitbranchWatcher = new ChildWatcher(this);
        Thread quitbranchWatcherthread = new Thread();
        quitbranchWatcherthread.start();
        this.quitbranchWatcher = quitbranchWatcher;
        List<String> quitchildren = zkeeper.getChildren(quitpath, this.quitbranchWatcher, null);

        if (!quitchildren.isEmpty()) {
            removeUser();
        }

        //initialize the ChildWatcher of online
        ChildWatcher onlinebranchWatcher = new ChildWatcher(this);
        Thread onlinebranchWatcherthread = new Thread();
        onlinebranchWatcherthread.start();
        this.onlinebranchWatcher = onlinebranchWatcher;
        List<String> onlinechildren = zkeeper.getChildren(onlinepath, this.onlinebranchWatcher, null);

        if (!onlinechildren.isEmpty()) {
            logger.info("Current online users: " + onlinechildren);
            createKafkaTopic();
        }
    }


    private void closeConnection() throws InterruptedException {
        zkeeper.close();
    }


    private void createZkTreeStructure() {
        try {
            //iterate over the TreeStructure and create the nodes
            for (String node : TreeStructure) {
                Stat exists = null;
                try {
                    exists = zkeeper.exists(node, true);
                } catch (KeeperException e) {
                    logger.error(e.code());
                }
                if (exists == null) {
                    zkeeper.create(node, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    logger.info("created " + node);
                } else {
                    logger.warn(node + " already exists.");
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);

        } catch (KeeperException e) {
            e.printStackTrace();
            logger.error(e.code());
        }
    }

    /*
    The method is synchronized, because the manager and the branchwatcher can call it.
    The synchronized ensures that the mentioned threads does not interleave.
    Like this, only one thread at a time can register the same new user.
     */
    public synchronized void registerUser() {
        List<String> children;
        try {
            //get all the current enrollrequests
            children = zkeeper.getChildren(enrollpath, enrollbranchWatcher, null);
            if (!children.isEmpty()) {
                logger.info("Current enroll requests: " + children);

                //process the enrollrequests one by one
                for (String child : children) {

                    //check if the enrollrequest still exists and if yes, retrieve the nodedata
                    Stat stillexists = zkeeper.exists(enrollpath + "/" + child, false);
                    if (stillexists != null) {
                        byte data[] = zkeeper.getData(enrollpath + "/" + child, null, null);
                        String nodedata = new String(data);

                        //if the data of the enrollrequest equals -1
                        if (nodedata.equals("-1")) {

                            //check if there is already a node in the registry for the new user
                            Stat exists = zkeeper.exists(registrypath + "/" + child, null);
                            if (exists == null) {
                                //if the user does not exist in the registry yet, create a new node in the registry
                                try {
                                    zkeeper.create(registrypath + "/" + child, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                                    //if there is a problem creating the node
                                } catch (KeeperException | InterruptedException e) {
                                    e.printStackTrace();
                                    logger.warn("An error occurred. The user " + child + " could not be registered");
                                    //set the enrollment request to 0 --> create failed
                                    int version = zkeeper.exists(enrollpath + "/" + child, null).getVersion();
                                    zkeeper.setData(enrollpath + "/" + child, "0".getBytes(), version);
                                }

                                //success: set the enrollment request to 1
                                int version = zkeeper.exists(enrollpath + "/" + child, null).getVersion();
                                zkeeper.setData(enrollpath + "/" + child, "1".getBytes(), version);
                                logger.info("The new user " + child + " is successfully registered");


                            } else {
                                //if the user exists in the registry already
                                logger.warn("The user " + child + " is already registered.");
                                //set the enrollment request to 2 --> user already registered
                                int version = zkeeper.exists(enrollpath + "/" + child, null).getVersion();
                                zkeeper.setData(enrollpath + "/" + child, "2".getBytes(), version);
                            }
                        }
                    }
                }
            }

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    /*
    The method is synchronized, because the manager and the branchwatcher can call it.
    The synchronized ensures that the mentioned threads does not interleave.
    Like this, only one thread at a time can remove the same user.
     */
    public synchronized void removeUser() {
        List<String> children;
        try {
            children = zkeeper.getChildren(quitpath, quitbranchWatcher, null);
            if (!children.isEmpty()) {
                logger.info("Current quit requests: " + children);

                //process the quitrequests
                for (String child : children) {

                    //check if the quitrequest still exists and if yes, retrieve the nodedata
                    Stat stillexists = zkeeper.exists(quitpath + "/" + child, null);
                    if (stillexists != null) {
                        byte data[] = zkeeper.getData(quitpath + "/" + child, null, null);
                        String nodedata = new String(data);

                        if (nodedata.equals("-1")) {


                            //check if there is a node in the registry for the user who wants to quit
                            Stat exists = zkeeper.exists(registrypath + "/" + child, null);
                            if (exists != null) {
                                try {
                                    //get the version and delete the user from the registry
                                    int version = zkeeper.exists(registrypath + "/" + child, null).getVersion();
                                    zkeeper.delete(registrypath + "/" + child, version);

                                    //if there is a problem deleting the node
                                } catch (KeeperException | InterruptedException e) {
                                    e.printStackTrace();
                                    logger.warn("An error occurred. The user " + child + " could not be deleted.");
                                    //set the enrollment request to 0 --> delete failed
                                    int version = zkeeper.exists(quitpath + "/" + child, null).getVersion();
                                    zkeeper.setData(quitpath + "/" + child, "0".getBytes(), version);
                                }

                                //get the version and set the quit request to 1 --> success
                                int version = zkeeper.exists(quitpath + "/" + child, null).getVersion();
                                zkeeper.setData(quitpath + "/" + child, "1".getBytes(), version);
                                logger.info("The user " + child + " is removed.");


                                deleteKafkaTopic(child);
                                registeredUsers = getRegisteredUsers();
                                logger.info("System State update: registered users: " + registeredUsers);


                            } else {
                                logger.warn("The user " + child + " is not registered.");
                                //set the quit request to 2 --> user not registered
                                int version = zkeeper.exists(quitpath + "/" + child, null).getVersion();
                                zkeeper.setData(quitpath + "/" + child, "2".getBytes(), version);
                            }
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public List<String> getRegisteredUsers() {

        List<String> registeredusers = null;
        try {
            registeredusers = zkeeper.getChildren(registrypath, null, null);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return registeredusers;
    }

    void createKafkaTopic() {

        List<String> onlineusers = null;
        try {
            onlineusers = zkeeper.getChildren(onlinepath, onlinebranchWatcher, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        if (onlineusers != null) {
            logger.info("Current online users: " + onlineusers);
            for (String user : onlineusers) {
                //check if the user is registered in the registry
                try {
                    Stat registered = zkeeper.exists(registrypath + "/" + user, null);

                    if (registered != null) {

                        Stat knownuser = zkeeper.exists("/brokers/topics/" + user, null);

                        if (knownuser == null) {
                            //KAFKA
                            logger.info("Create KAFKA Topic");

                            Properties props = new Properties();
                            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    "localhost:9092");
                            props.put("acks", "all");
                            props.put("retries", 0);
                            props.put("batch.size", 16384);
                            props.put("buffer.memory", 33554432);
                            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                            props.put("value.serializer",
                                    "org.apache.kafka.common.serialization.StringSerializer");
                            KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);
                            String topic = user;
                            int partition = 0;

                            String key = "newuserKey";
                            String value = "Welcome to the chat" + user + "!";
                            prod.send(new ProducerRecord<String, String>(topic, partition, key, value));
                            prod.close();

                            logger.info("New Kafka topic for " + user + " created.");
                        }

                    } else {
                        logger.warn("The user " + user + " is not registered yet. Therefore the topic cannot be created");
                    }

                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }



    public void deleteKafkaTopic(String user) {
        //check if the user is registered in the registry
        try {
            Stat registered = zkeeper.exists(registrypath + "/" + user, null);



                Stat knownuser = zkeeper.exists("/brokers/topics/" + user, null);

                if (knownuser != null) {
                    //delete KAFKA topic
                    try {
                        //delete the topic and its subfolders
                        int version = zkeeper.exists("/brokers/topics/" + user + "/partitions/0/state", null).getVersion();
                        zkeeper.delete("/brokers/topics/" + user + "/partitions/0/state", version);

                        version = zkeeper.exists("/brokers/topics/" + user + "/partitions/0", null).getVersion();
                        zkeeper.delete("/brokers/topics/" + user + "/partitions/0", version);

                        version = zkeeper.exists("/brokers/topics/" + user + "/partitions", null).getVersion();
                        zkeeper.delete("/brokers/topics/" + user + "/partitions", version);

                        version = zkeeper.exists("/brokers/topics/" + user, null).getVersion();
                        zkeeper.delete("/brokers/topics/" + user, version);


                        //if there is a problem deleting the topic
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                        logger.warn("An error occurred. The topic " + user + " could not be deleted.");
                    }

                    logger.info("Kafka topic for " + user + " deleted.");
                }
                else{
                    logger.info("There is no Kafka topic for " + user);
                }


        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void run() {
        try {
            synchronized (this) {
                while (alive) {
                    wait();
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            try {
                this.closeConnection();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


