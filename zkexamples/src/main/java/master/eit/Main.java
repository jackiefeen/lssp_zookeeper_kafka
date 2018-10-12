package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class Main {
    private static final Logger logger = LogManager.getLogger("Main Class");

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        //first, create the manager and establish the connection to ZooKeeper
        Manager manager = new Manager();

        //TODO: Only create the tree structure in a new environment. How could be implement this? Right now, the errors of existing nodes are handled but it slows down the applciation
        manager.createZkTreeStructure();


        manager.closeConnection();
        logger.info("Connection Closed");
        System.exit(0);

    }
}
