package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Manager {
    private static final Logger logger = LogManager.getLogger("Manager Class");
    private static ZooKeeper zkeeper;
    private static ZKConnection zkConnection;
    private static final String[] TreeStructure = {"/request", "/request/enroll", "/request/quit", "/registry", "/online"};

    //constructor of the Manager
    public Manager() throws IOException, InterruptedException {
        initialize();
    }

    /*
    Functions to initialize, set up the manager and close the connection
     */
    private void initialize() throws IOException, InterruptedException {
        zkConnection = new ZKConnection();
        zkeeper = zkConnection.connect("localhost");
    }

    public void closeConnection() throws InterruptedException {
        zkeeper.close();
    }

    /*
    Functions for creating nodes
     */

    public void create(String path, byte[] data) throws InterruptedException, KeeperException {

        // check if the node exists already and create a new self-made watcher on it
        Stat exists = null;
        try {
            exists = zkeeper.exists(path, new SimpleWatcher());
            if (exists == null) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                logger.warn("the node you try to create already exists...");
            }

        } catch (KeeperException e) {
            logger.error(e.code());
        }
        Thread.sleep(2000);
    }

    public void createZkTreeStructure() {
        try {
            //iterate over the TreeStructure and create the nodes
            for (String node : TreeStructure) {
                create(node, null);
            }
        } catch (InterruptedException ex) {
            logger.warn(ex);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);

        } catch (KeeperException ex) {
            logger.error(ex.code());
        }
    }

    /*
    Functions for getting and setting the data on the nodes
     */
    public byte[] getData(String path, Watcher watcher, Stat stat) throws InterruptedException, KeeperException {

        byte[] b = null;
        b = zkeeper.getData(path, null, null);
        return b;
    }

    public void setData(String path, byte[] data, int version) throws InterruptedException, KeeperException {
        zkeeper.setData(path, data, zkeeper.exists(path, true).getVersion());
    }
}