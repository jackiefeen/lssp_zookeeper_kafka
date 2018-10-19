package master.eit.client;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class EnrollWatcher implements Watcher {

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType()== Watcher.Event.EventType.NodeDataChanged){
            System.out.println("Node data has been changed!");
        }
    }

}
