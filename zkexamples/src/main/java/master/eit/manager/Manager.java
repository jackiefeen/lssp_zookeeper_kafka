
package master.eit.manager;

import master.eit.ZKConnection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;


public class Manager implements Runnable {
    private static final Logger logger = LogManager.getLogger("Manager Class");
    private static ZooKeeper zkeeper;
    private static String enrollpath = "/request/enroll";
    private static String quitpath = "/request/quit";
    private static String onlinepath = "/online";
    private static String registrypath = "/registry";
    private static final String[] TreeStructure = {"/request", "/request/enroll", "/request/quit", "/registry", "/online"};
    public boolean alive = true;
    private Watcher branchWatcher;


    //constructor of the Manager
    public Manager(String hostPort) throws IOException, InterruptedException, KeeperException {

        //setup connection with zookeeper
        zkeeper = new ZKConnection().connect(hostPort);
        logger.info("State: " + zkeeper.getState());

        if (zkeeper != null) {
            //ensure that the enrollpath actually exists before monitoring it, if not, create the tree structure
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

        //initialize the BranchWatcher
        BranchWatcher branchWatcher = new BranchWatcher(this);
        Thread branchWatcherthread = new Thread();
        branchWatcherthread.start();
        this.branchWatcher = branchWatcher;
        List<String> children = zkeeper.getChildren(enrollpath, branchWatcher);
        if (!children.isEmpty()) {
            logger.info("Current enroll requests: " + children);
            registerUser();
        }
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
                    logger.warn( node + " already exists.");
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);

        } catch (KeeperException e) {
            e.printStackTrace();
            logger.error(e.code());
        }
    }


    public void registerUser() throws KeeperException, InterruptedException {
        List<String> children = null;
        Stat stat = new Stat();
        try {
            children = zkeeper.getChildren(enrollpath, branchWatcher, stat);

            if (!children.isEmpty()) {
                logger.info("Current enroll requests: " + children);
                //process the enrollrequests
                for (String child : children) {
                    byte data[] = zkeeper.getData(enrollpath + "/" + child, true, null);
                    String nodedata = new String(data);

                    if (nodedata.equals("-1")) {
                        Stat exists = null;
                        //check if there is already a node in the registry for the new user
                        exists = zkeeper.exists("/registry/" + child, true);

                        if (exists == null) {
                            //create a new node in the registry
                            try {
                                zkeeper.create("/registry/" + child, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                                //if there is a problem creating the node
                            } catch (KeeperException e) {
                                e.printStackTrace();
                                logger.warn("An error occurred. The user" + child + "could not be registered");
                                //set the enrollment request to 0 --> create failed
                                int version = zkeeper.exists(enrollpath + "/" + child, true).getVersion();
                                zkeeper.setData(enrollpath + "/" + child, "0".getBytes(), version);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                logger.warn("An error occurred. The user" + child + "could not be registered");
                                //set the enrollment request to 0 --> create failed
                                int version = zkeeper.exists(enrollpath + "/" + child, true).getVersion();
                                zkeeper.setData(enrollpath + "/" + child, "0".getBytes(), version);
                            }

                            //set the enrollment request to 1 --> success
                            int version = zkeeper.exists(enrollpath + "/" + child, true).getVersion();
                            zkeeper.setData(enrollpath + "/" + child, "1".getBytes(), version);
                            logger.info("The new user " + child + " is successfully registered");

                        } else {
                            logger.warn("The user " + child + " is already registered...");
                            //set the enrollment request to 2 --> user already registered
                            int version = zkeeper.exists(enrollpath + "/" + child, true).getVersion();
                            zkeeper.setData(enrollpath + "/" + child, "2".getBytes(), version);
                        }
                    }
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void run() {
        try {
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


