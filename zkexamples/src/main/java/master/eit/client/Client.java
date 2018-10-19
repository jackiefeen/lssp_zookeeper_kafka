package master.eit.client;

import master.eit.manager.SimpleWatcher;
import master.eit.ZKConnection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class Client implements Runnable {
    private static ZooKeeper zkeeper;
    private static String enrollpath = "/request/enroll";
    private static String username;
    public boolean alive = true;
    private static final Logger logger = LogManager.getLogger("Client Class" + username);
    //static Semaphore semaphore = new Semaphore(0);


    // constructor that connects to ZooKeeper and tries to enroll/register
    public Client(String hostPort, String username) throws IOException, InterruptedException, KeeperException {
        this.username = username;
        zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State: " + zkeeper.getState());

        if (zkeeper != null) {
            createEphemeralNode(enrollpath + "/" + username, "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            handleenrollrequest();
        }
    }

    /* Functions for creating nodes */
    private void createEphemeralNode(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws InterruptedException, KeeperException {
        // check if the node exists already and create a new self-made watcher on it
        Stat exists;
        try {
            exists = zkeeper.exists(path, new SimpleWatcher());
            if (exists == null) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.info("Created an ephemeral node for the enrollment request");
            } else {
                logger.warn("You have already created an enrollment request. Please wait until it is approved.");
            }

        } catch (KeeperException e) {
            logger.error(e.code());
        }
        Thread.sleep(2000);
    }

    public void handleenrollrequest() throws KeeperException, InterruptedException {
        byte[] edata;
        edata = zkeeper.getData(enrollpath + "/" + username, new EnrollWatcher(), null);
        String enrolldata = new String(edata);
        logger.info(enrollpath + "/" + username);
        logger.info(enrolldata);

        if (enrolldata.equals("1")) {
            logger.info(username + ":the registration was successful.");

        } else if (enrolldata.equals("2")) {
            logger.info(username + ":the client was already registered.");

        } else {
            logger.info(enrolldata.getClass());
            logger.info(username + ":the registration failed.");

        }
        int version = zkeeper.exists(enrollpath + "/" + username, true).getVersion();
        zkeeper.delete(enrollpath + "/" + username, version);
        logger.info(enrollpath + "/" + username + " deleted.");
    }


    public void closeConnection() throws InterruptedException {
        zkeeper.close();
    }

    //run the client as a Thread
    public void run() {
        synchronized (this) {
            while (alive) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                } finally {
                    try {
                        this.closeConnection();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ;
                }
            }
        }
    }


    public static void main(String[] args) throws InterruptedException, KeeperException {

        Boolean flag = true;
        Scanner read = new Scanner(System.in);

        while (flag) {
            System.out.println("Insert the IMS IP in this format: Host:Port");
            String hostPort = read.nextLine();
            System.out.println("What is your nickname?");
            String username = read.nextLine();

            try {
                new Client(hostPort, username).run();
            } catch (IOException e) {
                System.out.println("For some reason the connection has been closed, " +
                        "maybe the server is offline or you made some mistake");
                flag = true;
            }
        }
    }
}


