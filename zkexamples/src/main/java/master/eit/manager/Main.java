package master.eit.manager;

import master.eit.manager.Manager;
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        Thread thread = new Thread(manager);
        thread.start();
        logger.info("The Manager has started.");

        while (true){
        }

    }
}
