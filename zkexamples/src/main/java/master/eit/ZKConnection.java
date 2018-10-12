package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKConnection {
    private static final Logger logger = LogManager.getLogger("ZooKeeper Connection Class");
    private ZooKeeper zoo;
    CountDownLatch connectionLatch = new CountDownLatch(1);

    public ZKConnection() {
    }

    /*
    Create connection to ZooKeeper server
     */

    public ZooKeeper connect(String host) throws IOException, InterruptedException {

        //TODO check if ZooKeeper is running and return readable exception if not
        ZooKeeper zoo = new ZooKeeper(host, 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            }
        });

        connectionLatch.await(10, TimeUnit.SECONDS);
        return zoo;
    }

    /*
    Close connection to ZooKeeper server
     */
    public void close() throws InterruptedException {
        zoo.close();
    }
}
