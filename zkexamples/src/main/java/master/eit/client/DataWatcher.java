package master.eit.client;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DataWatcher implements Runnable, Watcher {
    private static final Logger logger = LogManager.getLogger("DataWatcher");
    private Client currentclient;

    public DataWatcher(Client client) {
        this.currentclient = client;
    }

    public void process(WatchedEvent watchedEvent) {
        //Todo: remove the events that are not needed

        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            logger.info("The data on node " + watchedEvent.getPath() + " changed");

            //check where the event was triggered and call a Client method accordingly
            try {
                if (watchedEvent.getPath().contains("/request/enroll")) {
                    currentclient.handleregistration();
                } else if (watchedEvent.getPath().contains("/request/quit")) {
                    currentclient.handlequitting();
                } else {
                    logger.warn("No action defined for this path: " + watchedEvent.getPath());
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            logger.info(watchedEvent.getPath() + " deleted");
        } else if (watchedEvent.getType() == Event.EventType.NodeCreated) {
            logger.info(watchedEvent.getPath() + " changed");
        } else if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            logger.info(watchedEvent.getPath() + " children created or deleted.");
        }

    }


    public void run() {
        synchronized (this) {

        }
    }
}
