package master.eit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;


public class Main {
    private static final Logger logger = LogManager.getLogger("Main Class");

    public static void main(String[] args) {

        Manager manager = null;
        try {
            manager = new Manager("localhost:2181");
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        Thread thread = new Thread(manager);
        thread.start();
        logger.info("The Manager has caught up with requests and started up.");



        while (true){
            //TODO: Exit this while with a condition and not only with an exception.

            //TODO: every 30 seconds (?) create a state of the system and save it in the manager

        }
    }
}
