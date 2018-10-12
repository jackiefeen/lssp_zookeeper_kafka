package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class Manager implements Runnable {
    private static final Logger logger = LogManager.getLogger("Manager Class");
    private static ZooKeeper zkeeper;
    private static Watcher childrenWatcher;
    private static String enrollpath = "/request/enroll";
    private static final String[] TreeStructure = {"/request", "/request/enroll", "/request/quit", "/registry", "/online"};
    public boolean alive = true;

    //constructor of the Manager
    public Manager(String hostport) throws IOException, InterruptedException {

        //setup connection with zookeeper
        zkeeper = new ZKConnection().connect(hostport);
        logger.info("State: " + zkeeper.getState());
        if(zkeeper != null){

        /*
        create a watcher for ENROLLMENT
         */
        childrenWatcher = new Watcher(){
            public void process(WatchedEvent watchedEvent) {
                logger.info("Event received" + watchedEvent.toString());
                if (watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                    try {
                        List<String> children = zkeeper.getChildren(enrollpath, this);
                        logger.info("There is a new client!");
                        logger.info("The current clients are: " + children);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        //ensure that the enrollpath actually exists before monitoring it, if not, create the tree structure
        try {
            if(zkeeper.exists(enrollpath, false)==null){
                createZkTreeStructure();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        //set the watcher on the enrollpath
        try {
            List<String> children = zkeeper.getChildren(enrollpath,childrenWatcher);
            logger.info("Currently connected: " + children);
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    }


    private void closeConnection() throws InterruptedException {
        zkeeper.close();
    }

    /*
    Functions for creating nodes
     */
    private void createNode(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws InterruptedException, KeeperException {

        // check if the node exists already and create a new self-made watcher on it
        Stat exists;
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

    private void createZkTreeStructure() {
        try {
            //iterate over the TreeStructure and create the nodes
            for (String node : TreeStructure) {
                createNode(node, null, null, null);
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
    Functions for getting and setting the data on the nodes
     */
    public byte[] getData(String path, Watcher watcher, Stat stat) throws InterruptedException, KeeperException {

        byte[] b;
        b = zkeeper.getData(path, null, null);
        return b;
    }

    public void setData(String path, byte[] data, int version) throws InterruptedException, KeeperException {
        zkeeper.setData(path, data, zkeeper.exists(path, true).getVersion());
    }

    //create a manager Thread
    public void run() {
        synchronized (this){
            while (alive){
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    } finally{
                    try {
                        this.closeConnection();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}