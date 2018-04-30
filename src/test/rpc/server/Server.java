package test.rpc.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import test.rpc.services.ServiceProcessor;

import java.io.IOException;

/**
 * Abstract class for RPC Server implementations
 *
 * Date: 27.04.2018
 * Time: 12:08
 */
public abstract class Server extends Thread
{
  protected final Logger logger = LogManager.getLogger(getClass());

  protected final ServiceProcessor processor;
  protected volatile boolean started = false;
  protected int port;

  protected Server(int port, ServiceProcessor processor) {
    this.port = port;
    logger.info("Starting server on port: " + port);
    this.processor = processor;
  }

  public abstract void startup() throws IOException;

  public void shutdown() {
    logger.info("Shutting down server...");
    processor.shutdown();
    started = false;
  }
}
