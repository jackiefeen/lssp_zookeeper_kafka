package master.eit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class SimpleWatcher implements Watcher {
    public void process(WatchedEvent event) {

        if(event.getType() == Watcher.Event.EventType.NodeCreated){
            System.out.println(event.getPath() + " created");
        }else if(event.getType() == Watcher.Event.EventType.NodeDeleted){
            System.out.println(event.getPath() + " deleted");
        }else if(event.getType() == Watcher.Event.EventType.NodeDataChanged){
            System.out.println(event.getPath() + " changed");
        }else if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
            System.out.println(event.getPath() + " children created");
        }

    }
}
