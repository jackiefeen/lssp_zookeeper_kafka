package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class Client {
    private final Logger logger = LogManager.getLogger("Client Class");
    private static ZooKeeper zkeeper;
    private String username;
    private String enrollpath = "/request/enroll";
    private String quitpath = "/request/quit";
    private String onlinepath = "/online";
    private String registrypath = "/registry";
    private static boolean alive = true;


    //client constructor
    private Client(String hostPort, String username) throws IOException, InterruptedException {
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


    private void register() {
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

    private void quit() {
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

    private void goOnline() {
        if (zkeeper != null) {
            try {
                Stat registered = zkeeper.exists(registrypath + "/" + username, null);
                if (registered != null) {

                    Stat exists = zkeeper.exists(onlinepath + "/" + username, null);
                    if (exists == null) {
                        zkeeper.create(onlinepath + "/" + username, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        logger.info("You are online now.");
                    } else {
                        logger.warn("You are already online - no need to go online again.");
                    }
                }
                else{
                    logger.warn("You are not registered yet. Please register before you go online");
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }


    private List<String> getOnlineusers(){
        List<String> onlineusers = null;
        try {
            onlineusers = zkeeper.getChildren(onlinepath, null, null);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Online users: " + onlineusers);
        return onlineusers;
    }

    void sendMessage(){
        //Todo: Implement sendMessage
    }

    void readMessages(){
        //Todo: Implement readMessages
    }


    private void goOffline() throws InterruptedException {
        zkeeper.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        Scanner read = new Scanner(System.in);

        System.out.println("Insert the IMS IP in this format: Host:Port");
        String hostPort = read.nextLine();
        System.out.println("What is your username?");
        String username = read.nextLine();
        Client client = new Client(hostPort, username);

        while (alive) {
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
            if (zkeeper.getState() == ZooKeeper.States.CLOSED) {
                alive = false;
            }
            Thread.sleep(5000);
        }
    }

}




