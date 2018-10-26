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
    private static String username;
    private static boolean alive = true;
    private final Logger logger = LogManager.getLogger("Client Class");
    private NodeDataWatcher nodeDataWatcher;


    // constructor that connects to ZooKeeper and tries to enroll/register
    private Client(String hostPort, String username) throws IOException, InterruptedException {
        this.username = username;
        this.alive = true;

        //connect to ZooKeeper
        this.zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State of the connection: " + zkeeper.getState());

        //instantiate the NodeDataWatcher
        this.nodeDataWatcher = new NodeDataWatcher(this);
        Thread nodeDataWatcherThread = new Thread(nodeDataWatcher);
        nodeDataWatcherThread.start();
    }

    /* Functions for creating nodes */
    private void createEphemeralNode(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        Stat exists;
        try {
            exists = zkeeper.exists(path, false);
            if (exists == null) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                exists = zkeeper.exists(path, nodeDataWatcher);
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


    public void register() throws KeeperException, InterruptedException {

        if (zkeeper != null) {
            createEphemeralNode(enrollpath + "/" + username, "-1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            logger.warn("There is an issue with the ZooKeeper connection.");
        }
    }

    public void handleregistration() throws KeeperException, InterruptedException {
        byte[] edata;
        edata = zkeeper.getData(enrollpath + "/" + username, true, null);
        if (edata != null) {
            String enrolldata = new String(edata);

            if (enrolldata.equals("1")) {
                logger.info(username + ":the registration was successful.");

            } else if (enrolldata.equals("2")) {
                logger.info(username + ":the client was already registered.");

            } else {
                logger.info(username + ":the registration failed.");
            }

            Stat exist = null;
            exist = zkeeper.exists(enrollpath + "/" + username, false);
            if (exist != null) {
                int version = zkeeper.exists(enrollpath + "/" + username, false).getVersion();
                zkeeper.delete(enrollpath + "/" + username, version);
            }
        }
    }


    private void closeConnection() throws InterruptedException {
        zkeeper.close();
    }


    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        Scanner read = new Scanner(System.in);

        System.out.println("Insert the IMS IP in this format: Host:Port");
        String hostPort = read.nextLine();
        System.out.println("What is your nickname?");
        String username = read.nextLine();
        Client client = new Client(hostPort, username);

        while (alive) {
            System.out.println("What would you like to do?");
            String todo = read.nextLine();

            if (todo.equals("register")) {
                client.register();
                Thread.sleep(8000);
            } else if (todo.equals("goonline")) {
            } else if (todo.equals("close")) {
                client.closeConnection();
                if (zkeeper.getState() == ZooKeeper.States.CLOSED) {
                    alive = false;
                }
            } else {
                System.out.println("Please chose to either register, goonline or close");
            }

        }

    }
}




