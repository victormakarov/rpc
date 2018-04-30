package test.rpc.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import test.rpc.Call;

import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Class implements RPC processing
 *
 * Incoming call-requests are processed in separate threads via executor (thread-pool). One request - one thread.
 * ServiceManager instance (provided in constructor) is used to process incoming call-request to Services
 * registered in it.
 *
 * Date: 27.04.2018
 * Time: 15:47
 */
public class ServiceProcessor
{
  private final Logger logger = LogManager.getLogger(this.getClass());

  private final ServiceManager manager;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  private volatile boolean needExit = false;

  public ServiceProcessor(ServiceManager manager) {
    this.manager = manager;
  }

  public void call(Call call, Consumer<Call> consumer) {
    if (needExit) return;
    executor.submit(() -> {
      manager.call(call);
      consumer.accept(call);
    });
  }

  public void shutdown() {
    needExit = true;
    try {
      logger.info("Shutting down executor");
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Shutdown interrupted");
    } finally {
      if (!executor.isTerminated()) {
        logger.warn("Cancel non-finished tasks");
      }
      executor.shutdownNow();
      logger.info("Shutdown finished");
    }
  }
}
