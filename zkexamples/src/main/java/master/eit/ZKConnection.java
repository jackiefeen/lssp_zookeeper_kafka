package master.eit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKConnection
{
    private ZooKeeper zoo;
    CountDownLatch connectionLatch = new CountDownLatch(1);

    public ZKConnection () {}

    public ZooKeeper connect(String host) throws IOException, InterruptedException {

        ZooKeeper zoo = new ZooKeeper(host, 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    connectionLatch.countDown();
                }
            }
        });

        connectionLatch.await(10, TimeUnit.SECONDS);

        return zoo;
    }

    public void close() throws InterruptedException{
        zoo.close();
    }
}
