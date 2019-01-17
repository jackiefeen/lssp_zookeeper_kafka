package master.eit;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;


import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;


public class Manager implements Runnable {
    private static final Logger logger = LogManager.getLogger("Manager Class");
    private static ZooKeeper zkeeper;
    private static String enrollpath = "/request/enroll";
    private static String quitpath = "/request/quit";
    private static String onlinepath = "/online";
    private static String registrypath = "/registry";
    private static String kafkatopicspath = "/brokers/topics";
    private static final String[] TreeStructure = {"/request", "/request/enroll", "/request/quit", "/registry", "/online"};
    private static final String[] ChatRooms = {"chatroom-EITDigital", "chatroom-LargeScaleSystemsProject", "chatroom-Sports"};
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
            //ensure that the tree structure in ZooKeeper exists, if not, create it
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

        /*
        The following initializes watchers and gets the current status of the system
        This is especially important if the manager starts up after the clients
         */

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

        //create the predefined chatrooms in Kafka
        createKafkaChatRooms();
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
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            logger.error(e);
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

                                    //check if there is a node in the registry for the user who wants to quit
                                    Stat online = zkeeper.exists(onlinepath + "/" + child, null);
                                    if (online != null) {
                                        //get the version and delete the user from the registry
                                        int onlineversion = zkeeper.exists(onlinepath + "/" + child, null).getVersion();
                                        zkeeper.delete(onlinepath + "/" + child, onlineversion);
                                    }

                                    //get the version and set the quit request to 1 --> success
                                    int quitversion = zkeeper.exists(quitpath + "/" + child, null).getVersion();
                                    zkeeper.setData(quitpath + "/" + child, "1".getBytes(), version);
                                    logger.info("The user " + child + " is removed.");

                                    //delete the Kafka topic of the user who has quit
                                    deleteKafkaTopic(child);
                                    registeredUsers = getRegisteredUsers();
                                    logger.info("System State update: registered users: " + registeredUsers);

                                    //if there is a problem deleting the node
                                } catch (KeeperException | InterruptedException e) {
                                    e.printStackTrace();
                                    logger.warn("An error occurred. The user " + child + " could not be deleted.");
                                    //set the quit request to 0 --> delete failed
                                    int version = zkeeper.exists(quitpath + "/" + child, null).getVersion();
                                    zkeeper.setData(quitpath + "/" + child, "0".getBytes(), version);
                                }


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

    //get all users that are currently registered in the system
    public List<String> getRegisteredUsers() {
        List<String> registeredusers = null;
        try {
            registeredusers = zkeeper.getChildren(registrypath, null, null);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return registeredusers;
    }

    //create a new topic in the ZooKeeper Tree for Kafka
    void createKafkaTopic() {
        List<String> onlineusers = null;
        //get all online users
        try {
            onlineusers = zkeeper.getChildren(onlinepath, onlinebranchWatcher, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        if (onlineusers != null) {
            logger.info("Current online users: " + onlineusers);

            //check for every user that is online if he is registered
            for (String user : onlineusers) {

                try {
                    Stat registered = zkeeper.exists(registrypath + "/" + user, null);

                    if (registered != null) {

                        //check if there is already a kafka topic for the user
                        Stat knownuser = zkeeper.exists(kafkatopicspath + "/" + user, null);

                        if (knownuser == null) {
                            //create a new Kafka topic
                            logger.info("Create KAFKA topic for " + user);

                            Properties properties = new Properties();
                            properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

                            //create a new Kafka Admin client to create the topic
                            AdminClient adminClient = AdminClient.create(properties);
                            ArrayList<NewTopic> topics = new ArrayList<>();
                            NewTopic newtopic = new NewTopic(user, 2, (short) 1);
                            topics.add(newtopic);
                            adminClient.createTopics(topics);
                            CreateTopicsResult result = adminClient.createTopics(topics);

                            /*
                            get the create topics result future back from creating the topic
                            and wait for the deletion to be complete (get())
                            before printing the success log and closing the adminClient
                             */
                            result.all().get();

                            logger.info("New Kafka topic for " + user + " created.");
                            adminClient.close();
                        }

                    } else {
                        logger.warn("The user " + user + " is not registered yet. Therefore the topic cannot be created");
                    }

                } catch (KeeperException | InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void deleteKafkaTopic(String user) {
        //check if there exists a Kafka topic for the user
        try {
            Stat knownuser = zkeeper.exists(kafkatopicspath + "/" + user, null);

            if (knownuser != null) {
                //if the Kafka topic exists, delete it
                logger.info("Delete KAFKA topic for user " + user);

                Properties properties = new Properties();
                properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

                //create a new Kafka Admin client to delete the topic
                AdminClient adminClient = AdminClient.create(properties);
                ArrayList<String> topics = new ArrayList<>();
                topics.add(user);
                DeleteTopicsResult result = adminClient.deleteTopics(topics);

                /*get the create topics result future back from creating the topic
                and wait for the deletion to be complete (get())
                before printing the success log and closing the adminClient
                */
                result.all().get();
                logger.info("Kafka topic for user " + user + " deleted.");
                adminClient.close();

            } else {
                logger.info("There is no Kafka topic for " + user);
            }

            //if there is a problem deleting the topic
        } catch (KeeperException | InterruptedException | ExecutionException e) {
            logger.info("There was a problem deleting the Kafka topic for " + user);

    }
    }

    private void createKafkaChatRooms() {
        try {
            //iterate over the predefined ChatRooms and create the nodes if they do not exist yet
            for (String node : ChatRooms) {
                Stat chatroomexists = null;

                chatroomexists = zkeeper.exists(kafkatopicspath + "/" + node, null);

                if (chatroomexists == null) {

                    logger.info("Create KAFKA topic for chatroom " + node);

                    Properties properties = new Properties();
                    properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

                    //create a new Kafka Admin client to create the topic
                    AdminClient adminClient = AdminClient.create(properties);
                    ArrayList<NewTopic> topics = new ArrayList<>();
                    NewTopic newtopic = new NewTopic(node, 2, (short) 1);
                    topics.add(newtopic);
                    CreateTopicsResult result = adminClient.createTopics(topics);

                    /*
                    get the future back from creating the topic and wait for the deletion to be complete (get())
                    before printing the success log and closing the adminClient
                     */
                    result.all().get();
                    logger.info("New Kafka topic for chatroom " + node + " created.");
                    adminClient.close();

                } else {
                    logger.warn("Chatroom " + node + " already exists.");
                }
            }
        } catch (InterruptedException | KeeperException | ExecutionException e) {
            e.printStackTrace();
            logger.error(e);
        }
    }


    public void run() {
        try {
            //let the manager run and only terminate it due to an exception
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


