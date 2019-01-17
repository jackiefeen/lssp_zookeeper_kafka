package master.eit;

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

        //essential for the client: watch if the data of enroll or quit has changed
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            logger.info("The data on node " + watchedEvent.getPath() + " changed");

            /*
            check where the event was triggered and call a Client method accordingly
            if the data on the request nodes has changed, this means that the manager has processed the request
             */
            try {
                if (watchedEvent.getPath().contains("/request/enroll")) {
                    currentclient.handleRegistration();
                } else if (watchedEvent.getPath().contains("/request/quit")) {
                    currentclient.handleQuitting();
                } else {
                    logger.warn("No action defined for this path: " + watchedEvent.getPath());
                }

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

            //check for the different WatchEvents related to a change in a node and log to console
        } else if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            logger.info(watchedEvent.getPath() + " deleted");
        } else if (watchedEvent.getType() == Event.EventType.NodeCreated) {
            logger.info(watchedEvent.getPath() + " changed");
        } else if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            logger.info(watchedEvent.getPath() + " children created or deleted.");

            /*
            check where the event was triggered and call a Client method accordingly
            if the online users have changed, get the new list of online users
             */
            if (watchedEvent.getPath().contains("/online")) {
                currentclient.getOnlineusers();
            }
            else {
                logger.info("No action defined for this path: " + watchedEvent.getPath());
            }

        }

    }


    public void run() {
        synchronized (this) {

        }
    }
}
