package master.eit.manager;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


public class BranchWatcher implements Runnable, Watcher {
    private static final Logger logger = LogManager.getLogger("BranchWatcher");
    private Manager currentmanager;

    public BranchWatcher(Manager manager) {
        this.currentmanager=manager;
    }


    public void process(WatchedEvent event) {

        if(event.getType() == Watcher.Event.EventType.NodeCreated){
            logger.info(event.getPath() + " created");
        }else if(event.getType() == Watcher.Event.EventType.NodeDeleted){
            logger.info(event.getPath() + " deleted");
        }else if(event.getType() == Watcher.Event.EventType.NodeDataChanged){
            logger.info(event.getPath() + " changed");
        }else if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
            logger.info(event.getPath() + " children created or deleted.");
            try {
                currentmanager.registerUser();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void run() {
    }
}
