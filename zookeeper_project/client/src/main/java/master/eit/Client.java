package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Client {
    private static final Logger logger = LogManager.getLogger("Client Class");
    private static ZooKeeper zkeeper;
    public String username;
    private String enrollpath = "/request/enroll";
    private String quitpath = "/request/quit";
    private String onlinepath = "/online";
    private String registrypath = "/registry";
    private String topics = "/brokers/topics";
    private static boolean alive = true;
    public static ClientGUI form = null;
    private Watcher onlineWatcher;

    //client constructor
    Client(String hostPort, String username) throws IOException, InterruptedException {
        this.username = username;

        //connect to ZooKeeper
        zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State of the connection: " + zkeeper.getState());
    }

    //create an ephemeral node for a request: e.g. request or quit
    private void createRequestNode(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        try {
            Stat exists = zkeeper.exists(path, null);
            if (exists == null) {

                //instantiate the DataWatcher for the new node
                DataWatcher dataWatcher = new DataWatcher(this);
                Thread nodeDataWatcherThread = new Thread(dataWatcher);
                nodeDataWatcherThread.start();

                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                zkeeper.exists(path, dataWatcher);
                logger.info("Created an ephemeral node on " + path + " for the request");
            } else {
                logger.warn("You have already created an enrollment request. Please wait until it is approved.");
                // Set status to tried multiple times
                ClientGUI.registrationstatus = "3";
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("An error occurred.");
        }
    }

    //create an ephemeral node for the registration request and set a datawatcher on it
    public void register() {
        if (zkeeper != null) {
            createRequestNode(enrollpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    /*if the nodedata of the registration request was changed by the manager, i.e. the manager has processed the request,
    the nodedatawatcher was triggered
    and the client now needs to act upon the new data and handle the registration
     */
    String handleRegistration() throws KeeperException, InterruptedException {
        byte[] edata = zkeeper.getData(enrollpath + "/" + username, true, null);
        if (edata != null) {
            String enrolldata = new String(edata);
            logger.info(enrolldata);

            //check the data of the node and display the result of the request
            if (enrolldata.equals("1")) {
                logger.info(username + ": the registration was successful.");
                ClientGUI.registrationstatus = enrolldata;

            } else if (enrolldata.equals("2")) {
                logger.info(username + ": the client was already registered.");
                ClientGUI.registrationstatus = enrolldata;

            } else {
                logger.info(username + ": the registration failed.");
                ClientGUI.registrationstatus = enrolldata;
            }
            // delete the enrollment request from the ZooKeeper tree
            Stat exist = zkeeper.exists(enrollpath + "/" + username, null);
            if (exist != null) {
                int version = zkeeper.exists(enrollpath + "/" + username, null).getVersion();
                zkeeper.delete(enrollpath + "/" + username, version);
            }
        }
        return ClientGUI.registrationstatus;
    }

    //create an ephemeral node for the quit request and set a datawatcher on it
    public void quit() {
        if (zkeeper != null) {
            createRequestNode(quitpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    /*if the nodedata of the quit request was changed by the manager, i.e. the manager has processed the request,
    the nodedatawatcher was triggered
    and the client now needs to act upon the new data and handle the quitting
     */
    void handleQuitting() throws KeeperException, InterruptedException {
        byte[] edata = zkeeper.getData(quitpath + "/" + username, true, null);
        if (edata != null) {
            String enrolldata = new String(edata);

            //check the data of the node and display the result of the request
            if (enrolldata.equals("1")) {
                logger.info(username + ": the quit was successful.");

            } else if (enrolldata.equals("2")) {
                logger.info(username + ": the quit was successful.");

            } else {
                logger.info(username + ": the quit failed.");
            }

            // delete the quit request from the ZooKeeper tree
            Stat exist = zkeeper.exists(quitpath + "/" + username, null);
            if (exist != null) {
                int version = zkeeper.exists(quitpath + "/" + username, null).getVersion();
                zkeeper.delete(quitpath + "/" + username, version);
            }
        }
    }

    /*
    enables the client to go online in the chatroom
     */
    public int goOnline() {
        //check if there is an existing zookeeper connection
        if (zkeeper != null) {
            try {
                //before the user can go online, he needs to be registered --> check if the user exists in the registry
                Stat registered = zkeeper.exists(registrypath + "/" + username, null);
                if (registered != null) {

                    //check if the user is already only
                    Stat exists = zkeeper.exists(onlinepath + "/" + username, null);
                    if (exists == null) {
                        zkeeper.create(onlinepath + "/" + username, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        logger.info("You are online now.");

                        //initialize the Watcher of the /online path to get alerted if new clients join
                        DataWatcher onlineWatcher = new DataWatcher(this);
                        Thread onlineWatcherthread = new Thread();
                        onlineWatcherthread.start();
                        this.onlineWatcher = onlineWatcher;
                        zkeeper.getChildren(onlinepath, this.onlineWatcher, null);

                    } else {
                        logger.warn("You are already online - no need to go online again.");
                    }
                    return 0;
                }
                else{
                    logger.warn("You are not registered yet. Please register before you go online");
                    return -1;
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error("Something went wrong - going online is not possible right now.");
            }
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
        return -1;
    }

    //refresh the list of onlineusers in the GUI
    public void refreshGUI(List<String> onlineusers){
        form.updateOnlineUsers(onlineusers);
    }

    //refresh the list of chatrooms in the GUI
    public void refreshGUI2(List<String> chatrooms){
        form.updateChatrooms(chatrooms);
    }

    //retrieve all current online users
    public List<String> getOnlineusers(){
        List<String> onlineusers = null;
        try {
            //get the children of the online branch in zookeeper and keep a watcher on the branch
            onlineusers = zkeeper.getChildren(onlinepath, this.onlineWatcher, null);
            //refresh the GUI with the new list of online users
            refreshGUI(onlineusers);

        } catch (KeeperException | InterruptedException e) {
            logger.error("The online users could not be retrieved");
        }
        logger.info("Online users: " + onlineusers);
        return onlineusers;
    }

    //retrieve all chatrooms
    public List<String> getOnlinechatrooms(){
        List<String> chatrooms = new ArrayList<String>();
        try {
            //get the children of the kafka topics branch in zookeeper
            List<String> children = zkeeper.getChildren(topics, null, null);
            if (!children.isEmpty()) {

                /*check for every node under the topics branch if it is a chatroom,
                 if yes, add it to the list of online chatrooms
                 */
                for (String child : children) {
                    if (child.contains("chatroom-")) {
                        chatrooms.add(child);
                    }
                }
            }
            //refresh the GUI with the new list of chatrooms
            refreshGUI2(chatrooms);

        } catch (KeeperException | InterruptedException e) {
            logger.error("The chatrooms could not be retrieved");
        }
        logger.info("Online chatrooms: " + chatrooms);
        return chatrooms;
    }

    //enables the client to send a message to another user or to a chatroom
    public String sendMessage(String direction, String sender, String topic, String msg){
        String msgsent = "";
        //instantiate a new Kafka Producer and use the producer to send the message
        KProducer producer = new KProducer();
        try {
            msgsent = producer.sendMessage(1, direction, sender, topic, msg);
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("Something wrong happened in sendMessage()");
        }

        return msgsent;
    }

    //enables the client to read the messages in their 1:1 chats with other users or in a chatroom
    public Thread readMessages(String topic, Integer parallelism){
        return new Thread(new Refresher(topic, parallelism));
    }

    //close the ZooKeeper connection of the client
    public void goOffline() {
        try {
            zkeeper.close();
            logger.info("DISCONNECTED");
        } catch (InterruptedException e) {
            logger.error("Error when Disconnecting");
        }
    }

    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, UnsupportedLookAndFeelException, InstantiationException, IllegalAccessException {

        //initialize the GUI
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                form = new ClientGUI();
                form.setVisible(true);
            }
        });

        //keep the GUI running until it is terminated
        while (alive) {
           try{
               if (zkeeper.getState() == ZooKeeper.States.CLOSED) {
                   alive = false;
               }
           } catch (Exception e){
               logger.info("Wating for connection to ZooKeeper...");
            }
            Thread.sleep(5000);
        }
    }

}




