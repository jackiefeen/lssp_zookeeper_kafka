package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class Client implements Runnable {
    private static final Logger logger = LogManager.getLogger("Client Class");
    private static ZooKeeper zkeeper;
    private static String enrollpath = "/request/enroll";


    //constructor that connects to ZooKeeper and tries to enroll/register
    public Client(String hostPort, String username) throws IOException, InterruptedException, KeeperException {

        zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State: " + zkeeper.getState());
        if (zkeeper != null){
            createEphemeralNode(enrollpath + "/" + username, "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }


    }

    /*
    Functions for creating nodes
     */

    private void createEphemeralNode(String path, byte[] data, List<ACL> acl,CreateMode createMode ) throws InterruptedException, KeeperException {
        // check if the node exists already and create a new self-made watcher on it
        Stat exists;
        try {
            exists = zkeeper.exists(path, new SimpleWatcher());
            if (exists == null) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.info("created an ephemeral node for the enrollment request");
            } else {
                logger.warn("You have already created an enrollment request. Please wait until it is approved.");
            }

        } catch (KeeperException e) {
            logger.error(e.code());
        }
        Thread.sleep(2000);
    }

    public void closeConnection() throws InterruptedException {
        zkeeper.close();
    }

    //run the client as a Thread
    public void run() {
        synchronized (this){
            while(true){
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
                finally{
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


    public static void main (String [] args) throws InterruptedException, IOException, KeeperException {
        /*
        if (args.length !=2){
            logger.error("Please pass the host:port and a username");
            System.exit(0);
        }
        String hostPort = args[0];
        String username = args[1];
        */
        //TODO: only for testing purposes: I pass a hard-coded hostPort and username
        String hostPort = "localhost:2181";
        String username = "jackie";
        //
        new Client(hostPort, username).run();
    }
}


