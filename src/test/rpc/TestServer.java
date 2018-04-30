package test.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import test.rpc.server.Server;
import test.rpc.services.ServiceManager;
import test.rpc.services.ServiceProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * Date: 27.04.2018
 * Time: 12:07
 */
public class TestServer
{
  private final static Logger logger = LogManager.getLogger(TestServer.class);

  private final static String SERVICES_FILENAME = "server.properties";

  private static Properties loadProperties(String fileName) throws IOException {
    Properties ps;
    try (InputStream input = TestServer.class.getClassLoader().getResourceAsStream(fileName)) {
      if (input == null)
        throw new IOException("Unable to find config file: " + fileName);
      ps = new Properties();
      ps.load(input);
    }
    return ps;
  }

  public static void main(String[] args)
  {
    int port = -1;
    boolean complex = false;
    if (args != null && (args.length == 1 || args.length == 2)) {
      try { port = Integer.valueOf(args[0]); } catch (NumberFormatException ne) {
        System.out.println("Invalid port: " + args[0]);
      }
      if (args.length == 2) {
        complex = "complex".equals(args[1]);
        if (!complex) port = -1;
      }
    }
    if (port <= 0) {
      System.out.println("Usage: TestServer <listen_port> [complex]");
      System.exit(-1);
    }

    Server server = null;
    try
    {
      final ServiceManager sm = new ServiceManager(loadProperties(SERVICES_FILENAME));
      final ServiceProcessor sp = new ServiceProcessor(sm);
      server = (complex) ? new test.rpc.server.complex.Server(port, sp):
                           new test.rpc.server.simple.Server(port, sp);
      server.startup();
      server.join();

    } catch (InterruptedException e) {
      logger.info("Interrupted. Exiting...");
    } catch (Exception e) {
      logger.error(e.getMessage());
    } finally {
      if (server != null) server.shutdown();
    }
    logger.info("Exit");
  }
}
