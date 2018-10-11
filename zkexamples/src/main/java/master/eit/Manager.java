package master.eit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Manager implements iManager {
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
        zkConnection.close();
    }

    public void create(String path, byte[] data) throws InterruptedException, KeeperException {

        zkeeper.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
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