package master.eit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class Main {
    private static final Logger logger = LogManager.getLogger("Main Class");

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Manager manager = new Manager();
        byte [] data = "abc".getBytes();
        manager.create("/masterdsc/student1", data);
        manager.closeConnection();
        logger.info("Connection Closed");

        System.exit(0);

    }

}
