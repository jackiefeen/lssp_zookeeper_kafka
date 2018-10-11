package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Manager {
    private static final Logger logger = LogManager.getLogger("Manager Class");
    private static ZooKeeper zkeeper;
    private static ZKConnection zkConnection;

    public Manager() throws IOException, InterruptedException {
        initialize();
    }

    private void initialize() throws IOException, InterruptedException {
        zkConnection = new ZKConnection();
        zkeeper = zkConnection.connect("localhost");
    }

    public void closeConnection() throws InterruptedException {
        zkeeper.close();
    }

    public void create(String path, byte[] data) throws InterruptedException, KeeperException {

        // check if the node exists already and create a new self-made watcher on it
        Stat exists= null;
        try {
            exists = zkeeper.exists(path, new SimpleWatcher());
            logger.info("Does the node exist?: " + exists);
            if(exists == null){
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            else{
                logger.warn("the node you wanna create already exists...");
            }

        } catch (KeeperException e) {
            e.printStackTrace();
            System.out.println("the node you wanna create already exists...");
        }
        Thread.sleep(2000);
    }

    public Object getZNodeData(String path, boolean watchFlag) throws InterruptedException, KeeperException {

        byte[] b = null;
        b = zkeeper.getData(path, null, null);
        try {
            return new String(b, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return "Empty";
        }
    }

    public void update(String path, byte[] data) throws InterruptedException, KeeperException {
        int version = zkeeper.exists(path, true).getVersion();
        zkeeper.setData(path, data, version);
    }
}