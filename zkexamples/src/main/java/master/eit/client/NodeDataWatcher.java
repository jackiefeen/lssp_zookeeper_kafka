package master.eit.client;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class NodeDataWatcher implements Runnable, Watcher {
    private static final Logger logger = LogManager.getLogger("NodeDataWatcher");
    private Client currentclient;

    public NodeDataWatcher(Client client) {
        this.currentclient = client;
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            logger.info("The data on node " + watchedEvent.getPath() + " changed");
            try {
                currentclient.handleregistration();
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


        public void run () {

        }
    }
