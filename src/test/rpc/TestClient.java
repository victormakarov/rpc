package test.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import test.rpc.client.Client;

/**
 * Date: 28.04.2018
 * Time: 0:36
 */
public class TestClient
{
  private final static Logger logger = LogManager.getLogger(TestClient.class);

  public static void main(String[] args)
  {
    String host = null;
    int port = -1;
    if (args != null && args.length == 2) {
      host = args[0];
      try { port = Integer.valueOf(args[1]); } catch (NumberFormatException ne) {
        System.out.println("Invalid port: " + args[1]);
      }
    }
    if (port <= 0 || host == null || host.length() <= 0) {
      System.out.println("Usage: TestClient <server_host> <server_port>");
      System.exit(-1);
    }

    try
    {
      Client client = new Client(host, port);
      client.startup();

      Object obj = client.call("service1", "mPubInteger4Integer", new Object[]{1000});
      logger.info("int mPubInteger4Integer(int) = " + obj);

      obj = client.call("service1", "mPubVoid", null);
      logger.info("void mPubVoid() = " + obj); // (obj == Void.TYPE)

      try {
        client.call("service1", "mPubVoidExc", null);
      } catch (Exception e) {
        //e.printStackTrace();
        logger.info("void mPubVoidExc(): " + e.getMessage() + ", Cause: " + e.getCause());
      }

      for(int i=0; i<10; i++) {
        new Thread(() -> {
          try {
            while(true) {
              client.call("service1", "sleep", new Object[] {new Long(1000)});
              logger.info("Current Date is: " + client.call("service1", "getCurrentDate", new Object[] {}));
            }
          } catch (Exception e) {
            logger.warn("Exception while call to client: " + e);
          }
        }).start();
      }

      try { Thread.sleep(60*1000); } catch (InterruptedException e) {
        logger.info("Interrupted. Exiting...");
      }
      client.shutdown();

    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    logger.info("Exit");
  }
}

