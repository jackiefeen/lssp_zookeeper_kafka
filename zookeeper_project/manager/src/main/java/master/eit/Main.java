package master.eit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.Semaphore;


public class Main {
    private static final Logger logger = LogManager.getLogger("Main Class");

    public static void main(String[] args) {
        Manager manager = null;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("\nYou killed me :'(");
            }
        });

        try {
            manager = new Manager("localhost:2181");
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        Thread thread = new Thread(manager);
        thread.start();
        logger.info("The Manager has caught up with requests and started up.");

        while (true) {

            try {
                System.out.println("Ctrl-C to shutdown the Manager");
                Thread.sleep(30000);
            } catch(InterruptedException e) {
                System.out.println("I cannot sleep, something woke me up");
            }

        }


    }
}
