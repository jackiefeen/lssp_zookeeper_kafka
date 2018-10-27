package master.eit.client;

import master.eit.ZKConnection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class Client {
    private static ZooKeeper zkeeper;
    private String enrollpath = "/request/enroll";
    private static String quitpath = "/request/quit";
    private String username;
    private static boolean alive = true;
    private final Logger logger = LogManager.getLogger("Client Class");


    // constructor that connects to ZooKeeper and tries to enroll/register
    private Client(String hostPort, String username) throws IOException, InterruptedException {
        this.username = username;

        //connect to ZooKeeper
        this.zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State of the connection: " + zkeeper.getState());

    }

    private void createEphemeralNode(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        try {
            Stat exists = zkeeper.exists(path, false);
            if (exists == null) {

                //instantiate the DataWatcher for the new node
                DataWatcher dataWatcher = new DataWatcher(this);
                Thread nodeDataWatcherThread = new Thread(dataWatcher);
                nodeDataWatcherThread.start();

                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                zkeeper.exists(path, dataWatcher);
                logger.info("Created an ephemeral node on " + path + " for the enrollment request");
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


    public void register(){
        if (zkeeper != null) {
            createEphemeralNode(enrollpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    public void handleregistration() throws KeeperException, InterruptedException {
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

            Stat exist = zkeeper.exists(enrollpath + "/" + username, false);
            if (exist != null) {
                int version = zkeeper.exists(enrollpath + "/" + username, false).getVersion();
                zkeeper.delete(enrollpath + "/" + username, version);
            }
        }
    }

    public void quit(){
        if (zkeeper != null) {
            createEphemeralNode(quitpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    public void handlequitting() throws KeeperException, InterruptedException {
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

            Stat exist = zkeeper.exists(quitpath + "/" + username, false);
            if (exist != null) {
                int version = zkeeper.exists(quitpath + "/" + username, false).getVersion();
                zkeeper.delete(quitpath + "/" + username, version);
            }
        }

    }


    private void closeConnection() throws InterruptedException {
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

            switch (todo){
                case "register":
                    client.register();
                    break;
                case "quit":
                    client.quit();
                    break;
                case "goonline":
                    break;
                case "close":
                    client.closeConnection();
                default: System.out.println("Please chose to either register, quit, goonline or close");
                    break;
            }
                if (zkeeper.getState() == ZooKeeper.States.CLOSED) {
                    alive = false;
                }
                Thread.sleep(3000);
        }
    }

}




