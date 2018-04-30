package test.rpc.server.simple;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import test.rpc.Call;
import test.rpc.CallStream;
import test.rpc.services.ServiceProcessor;

/**
 * Class implements simple Server for RPC processing.
 *
 * Method run() accepts new client connections and passes it for processing
 * in separated threads via executor (thread-pool). One client - one thread.
 *
 * ServiceProcessor instance (provided in constructor) is used to process incoming call-request.
 *
 * Date: 27.04.2018
 * Time: 16:27
 */
public class Server extends test.rpc.server.Server
{
  private ServerSocket listener;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  public Server(int port, ServiceProcessor processor) {
    super(port, processor);
  }

  public void startup() throws IOException
  {
    if (started) return;
    this.listener = new ServerSocket(port);
    this.start();

    while (!started) {
      try { synchronized (this) { this.wait(10); }}
      catch (InterruptedException e) { logger.trace(e); }
    }
  }

  @Override
  public void shutdown()
  {
    super.shutdown();

    try { listener.close(); } catch (IOException exc) {
      logger.error("Failed to close server socket. Details: " + exc.getMessage());
    }

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

  @Override
  public void run()
  {
    if (started) return;
    started = true;
    while (started) {
      logger.debug("Waiting for a new client...");
      try {
        Socket socket = listener.accept();
        if (socket != null) {
          if (!started) {
            try { socket.close(); } catch (Exception e) { logger.warn(e); }
          } else {
            logger.debug("New client socket accepted");
            executor.submit(() -> process(socket));
          }
        }
      } catch (IOException exc) {
        logger.error("Failed to accept new socket. Details: " + exc.getMessage());
      }
    }
    started = false;
  }

  private void process(Socket socket)
  {
    try (Socket in = socket; // to auto close it
         DataOutputStream os = new DataOutputStream(new BufferedOutputStream(in.getOutputStream()));
         DataInputStream is = new DataInputStream(new BufferedInputStream(in.getInputStream())))
    {
      while (started)
      {
        Call call;
        try {
          int seqNum = is.readInt();
          Call.Request request = CallStream.readRequest(is, seqNum);
          call = new Call(seqNum, request);
        } catch (EOFException e) {
          logger.info("Client socket=" + socket + " closed");
          break;
        }

        processor.call(call, (c) -> {
          synchronized (in) {
            try
            {
              os.writeInt(c.getSeqNum());
              CallStream.writePacket(os, c.getResponse());

              final String result = (c.getResponse().getResult() != null) ?
                                     c.getResponse().getResult().toString() :
                                     c.getResponse().getExc().toString();

              logger.info("Call seqNum=" + c.getSeqNum() + ". Result sent: " + result);

            } catch (Exception exc) {
              logger.error("Failed to write response to socket= " + socket + ". Details: " + exc.getMessage());
            }
          }
        });
      }
    } catch (Exception exc) {
      logger.error("Failed to process socket=" + socket + ". Details: " + exc.getMessage());
    }
  }

}
