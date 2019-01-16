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
    public Client(String hostPort, String username) throws IOException, InterruptedException {
        this.username = username;

        //connect to ZooKeeper
        this.zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State of the connection: " + zkeeper.getState());
    }

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
            }
        } catch (KeeperException e) {
            logger.error(e.code());
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);
        }
    }

    public void register() {
        if (zkeeper != null) {
            createRequestNode(enrollpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    void handleRegistration() throws KeeperException, InterruptedException {
        byte[] edata = zkeeper.getData(enrollpath + "/" + username, true, null);
        if (edata != null) {
            String enrolldata = new String(edata);

            if (enrolldata.equals("1")) {
                logger.info(username + ": the registration was successful.");

            } else if (enrolldata.equals("2")) {
                logger.info(username + ": the client was already registered.");

            } else {
                logger.info(username + ": the registration failed.");
            }
            Stat exist = zkeeper.exists(enrollpath + "/" + username, null);
            if (exist != null) {
                int version = zkeeper.exists(enrollpath + "/" + username, null).getVersion();
                zkeeper.delete(enrollpath + "/" + username, version);
            }
        }
    }

    public void quit() {
        if (zkeeper != null) {
            createRequestNode(quitpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    void handleQuitting() throws KeeperException, InterruptedException {
        byte[] edata = zkeeper.getData(quitpath + "/" + username, true, null);
        if (edata != null) {
            String enrolldata = new String(edata);

            if (enrolldata.equals("1")) {
                logger.info(username + ": the quit was successful.");

            } else if (enrolldata.equals("2")) {
                logger.info(username + ": the quit was successful.");

            } else {
                logger.info(username + ": the quit failed.");
            }

            Stat exist = zkeeper.exists(quitpath + "/" + username, null);
            if (exist != null) {
                int version = zkeeper.exists(quitpath + "/" + username, null).getVersion();
                zkeeper.delete(quitpath + "/" + username, version);
            }
        }
    }

    public int goOnline() {
        if (zkeeper != null) {
            try {
                Stat registered = zkeeper.exists(registrypath + "/" + username, null);
                if (registered != null) {

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
                e.printStackTrace();
            }
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
        return -1;
    }

    public void refreshGUI(List<String> onlineusers){
        form.updateOnlineUsers(onlineusers);
    }

    public void refreshGUI2(List<String> chatrooms){
        form.updateChatrooms(chatrooms);
    }

    public List<String> getOnlineusers(){
        List<String> onlineusers = null;
        try {
            onlineusers = zkeeper.getChildren(onlinepath, this.onlineWatcher, null);
            refreshGUI(onlineusers);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Online users: " + onlineusers);
        return onlineusers;
    }

    public List<String> getOnlinechatrooms(){
        List<String> chatrooms = new ArrayList<String>();
        try {
            List<String> children = zkeeper.getChildren(topics, null, null);
            if (!children.isEmpty()) {
                for (String child : children) {
                    if (child.contains("chatroom-")) {
                        chatrooms.add(child);
                    }
                }
            }
            refreshGUI2(chatrooms);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Online chatrooms: " + chatrooms);
        return chatrooms;
    }

    public String sendMessage(String direction, String sender, String topic, String msg){
        String msgsent = "";
        KProducer producer = new KProducer();
        try {
            msgsent = producer.sendMessage(1, direction, sender, topic, msg);
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("Something wrong happened in sendMessage()");
        }

        return msgsent;
    }

    public Thread readMessages(String topic){
        return new Thread(new KConsumer(topic));
    }

    public void goOffline() throws InterruptedException {
        zkeeper.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, UnsupportedLookAndFeelException, InstantiationException, IllegalAccessException {

        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                form = new ClientGUI();
                form.setVisible(true);
            }
        });


        /*
        Scanner read = new Scanner(System.in);
        System.out.println("Insert the IMS IP in this format: Host:Port");
        String hostPort = read.nextLine();
        System.out.println("What is your username?");
        String username = read.nextLine();

        Client client = new Client(hostPort, username);
        */


        while (alive) {
           /*
            System.out.println("What would you like to do?");
            String todo = read.nextLine();

            switch (todo) {
                case "register":
                    client.register();
                    break;
                case "quit":
                    client.quit();
                    break;
                case "goonline":
                    client.goOnline();
                    break;
                case "getonlineusers":
                    client.getOnlineusers();
                    break;
                case "gooffline":
                    client.goOffline();
                    break;
                default:
                    System.out.println("Please choose to either register, quit, goonline, getonlineusers or gooffline");
                    break;
            }
            */
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




