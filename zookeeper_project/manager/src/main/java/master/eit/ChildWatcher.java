package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


public class ChildWatcher implements Runnable, Watcher {
    private static final Logger logger = LogManager.getLogger("ChildWatcher");
    private Manager currentmanager;

    public ChildWatcher(Manager manager) {
        this.currentmanager = manager;
    }


    public void process(WatchedEvent event) {
        //check for the different WatchEvents related to a change in a node and log to console
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
            logger.info(event.getPath() + " created");
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            logger.info(event.getPath() + " deleted");
        } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            logger.info(event.getPath() + " changed");

            //essential for the manager: watch if the children of enroll or quit have changed
        } else if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            logger.info(event.getPath() + " children created or deleted.");

            //check where the node children changed event was triggered and call a Manager method accordingly
                // a new enrollrequest arrived - register the user
            if (event.getPath().contains("/request/enroll")) {
                currentmanager.registerUser();
                // a new quit request arrived - remove the user
            } else if (event.getPath().contains("/request/quit")) {
                currentmanager.removeUser();
            }
                // a new user went online - check if a new Kafka topic needs to be created
            else if(event.getPath().contains("/online")){
                currentmanager.createKafkaTopic();
            }
            else {
                logger.info("No action defined for this path: " + event.getPath());
            }
        }
    }

    public void run() {
        synchronized (this) {

        }
    }
}
