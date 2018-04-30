package test.rpc.server.complex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import test.rpc.Call;
import test.rpc.services.ServiceProcessor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.*;

/**
 *
 * Date: 28.04.2018
 * Time: 0:52
 */
public class Server extends test.rpc.server.Server
{
  public final static int IO_TIMEOUT = 100;

  public final static int READERS_COUNT = 10;
  public final static int WRITERS_COUNT = 10;

  public final static int MAX_IO_CHANNELS_COUNT = 20;

  private final LinkedList<RequestReader> readers = new LinkedList<>();
  private final LinkedList<ResponseWriter> writers = new LinkedList<>();

  private final HashMap<SocketChannel, RequestReader> readersByChannels = new HashMap<>();
  private final HashMap<SocketChannel, ResponseWriter> writersByChannels = new HashMap<>();

  public Server(int port, ServiceProcessor processor) {
    super(port, processor);
  }

  public void handleCall(Call call, SocketChannel channel)
  {
    //System.out.println("Call received: " + call);
    if (!started) {
      call.setExc(new IllegalStateException("Server is not started or exiting"));
      sendResponse(call, channel);
      return;
    }

    processor.call(call, (c) -> sendResponse(c, channel));
  }

  public void handleError(Exception exc, SocketChannel channel)
  {
    logger.error(exc.getMessage(), exc);
    closeChannel(channel);
  }

  private void sendResponse(Call call, SocketChannel channel)
  {
    try
    {
      getWriter(channel).send(call, channel);

      final String result = (call.getResponse().getResult() != null) ?
          call.getResponse().getResult().toString() :
          call.getResponse().getExc().toString();

      logger.info("Call seqNum=" + call.getSeqNum() + ". Result sent: " + result);

    } catch (Exception exc) {
      logger.error("Failed to send response. Details: " + exc.getMessage());
    }
  }

  public synchronized void startup() throws IOException
  {
    if (started) return;
    startupIO();
    try {
      acceptor = new Acceptor(new InetSocketAddress("0.0.0.0", port));
    } catch (IOException exc) {
      logger.error("Acceptor start error: " + exc.getMessage());
      shutdownIO();
      throw exc;
    }
    this.start();

    while (!started) {
      try { synchronized (this) { this.wait(10); }}
      catch (InterruptedException e) { logger.trace(e); }
    }
  }

  /**
   * Shuts down Server in background (non blocking).
   *
   * Shutdown sequence:
   * 1) Stop acceptor (close server socket).
   * 2) Signal readers to stop receive new requests and exit.
   * 3) Signal writers to send or cancel the rest of data and exit.
   */
  public synchronized void shutdown()
  {
    super.shutdown();
    acceptor.shutdown();
    shutdownIO();
  }

  /**
   * Implementation of Runable interface method (Thread class).
   */
  public void run()
  {
    if (started) return;
    started = true;
    acceptor.run();
  }

  private class Acceptor implements Runnable
  {
    private final Logger logger = LogManager.getLogger(this.getClass());

    private final ServerSocketChannel acceptorChannel;
    private final Selector selector;

    public Acceptor(SocketAddress address) throws IOException  {
      logger.info("Starting Acceptor...");
      acceptorChannel = ServerSocketChannel.open().bind(address);
      acceptorChannel.configureBlocking(false);
      selector = Selector.open();
      acceptorChannel.register(selector, SelectionKey.OP_ACCEPT);
      logger.info("Acceptor started on port: " + port);
    }

    public void shutdown() {
      try { if (acceptorChannel != null) acceptorChannel.close(); } catch (IOException exc) {
        logger.error("Failed to close acceptor channel. Details: " + exc.getMessage());
      }
      selector.wakeup();
    }

    private void closeAcceptedChannel(SocketChannel channel) {
      try { if (channel != null) channel.close(); } catch (IOException exc) {
        logger.warn("Failed to close accepted channel. Details: " + exc.getMessage());
      }
    }

    private void acceptChannel(SelectionKey key)
    {
      SocketChannel channel = null;
      try
      {
        if (!(key.channel() instanceof ServerSocketChannel))
          throw new IOException("Invalid key for accept");
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel)key.channel();
        if (serverSocketChannel != acceptorChannel)
          throw new IOException("Invalid server socket");

        channel = acceptorChannel.accept();
        if (channel != null) {
          channel.configureBlocking(false);
          Socket socket = channel.socket();
          final String host_port = socket.getRemoteSocketAddress().toString();
          logger.info("Incoming client connection from: " + host_port);
          boolean accepted = registerChannel(channel);
          if (!accepted)
            logger.warn("Failed to accept client connection from: " + host_port);
          else
            logger.info("Accepted new client connection from: " + host_port);
        }
      }
      catch (IOException io_exc) {
        logger.warn("Failed to accept client connection. Details: " + io_exc.getMessage());
        this.closeAcceptedChannel(channel);
      }
    }

    @Override
    public void run()
    {
      logger.info("Acceptor started");
      while (started)
      {
        try {
          selector.select();
        } catch (ClosedSelectorException closed_exc) {
          logger.error("Acceptor selector closed", closed_exc); break;
        } catch (IOException io_exc) {
          logger.error("Acceptor selector error", io_exc); break;
        }

        Iterator iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
          SelectionKey key = (SelectionKey)iterator.next();
          iterator.remove();
          if (key.isAcceptable()) this.acceptChannel(key);
          else {
            logger.error("Invalid key state on acceptor selector");
            key.cancel(); break;
          }
        }
      }

      try { if (selector != null) selector.close(); } catch (IOException selector_close_exc) {
        logger.error("Failed to close acceptor selector", selector_close_exc);
      }
      logger.info("Acceptor exited");
    }
  }
  private Acceptor acceptor;

  void closeChannel(SocketChannel channel) // Alse called from RequestReader (so make it package private)
  {
    if (channel == null) return;
    synchronized (readersByChannels) {
      RequestReader reader = readersByChannels.remove(channel);
      if (reader != null) reader.unregisterChannel(channel);
    }
    synchronized (writersByChannels) {
      ResponseWriter writer = writersByChannels.remove(channel);
      if (writer != null) writer.unregisterChannel(channel);
    }
    try {
      channel.close();
      logger.debug("Channel closed");
    } catch (IOException close_exc) {
      logger.warn("Failed to close socket channel. Details: " + close_exc.getMessage());
    }
  }

  private boolean registerChannel(SocketChannel channel)
  {
    try
    {
      registerForRead(channel);
      registerForWrite(channel);
    }
    catch (IOException register_exc) {
      logger.error("Failed to register new channel. Details: " + register_exc.getMessage());
      closeChannel(channel);
      return false;
    }
    return true;
  }

  private static class TaskComarator implements Comparator<IOTask> {
    @Override
    public int compare(IOTask t1, IOTask t2) {
      final int c1 = (t1 == null) ? 0 : t1.getChannelsCount();
      final int c2 = (t2 == null) ? 0 : t2.getChannelsCount();
      return c1 - c2;
    }
  }
  private static TaskComarator taskComarator = new TaskComarator();

  private void registerForWrite(SocketChannel channel) throws IOException
  {
    ResponseWriter writer;
    synchronized (writersByChannels) {
      writer = writersByChannels.get(channel);
      if (writer == null) {
        synchronized (writers) { // select busyless Writer
          Collections.sort(writers, taskComarator);
          writer = writers.peekFirst();
        }
        if (writer != null) writersByChannels.put(channel, writer);
      }
    }
    if (writer == null)
      throw new IOException("No one writer is created!");
    if (writer.getChannelsCount() > Server.MAX_IO_CHANNELS_COUNT)
      throw new IOException("Maximum count of writer channels exceeded");

    writer.registerChannel(channel);
  }

  private ResponseWriter getWriter(SocketChannel channel) throws IOException
  {
    ResponseWriter writer;
    synchronized (writersByChannels) {
      writer = writersByChannels.get(channel);
      if (writer == null)
        throw new IOException("Failed to obtain writer for channel: " + channel);
    }
    return writer;
  }

  private void registerForRead(SocketChannel channel) throws IOException
  {
    RequestReader reader;
    synchronized (readersByChannels) {
      reader = readersByChannels.get(channel);
      if (reader == null) {
        synchronized (readers) { // select busyless Reader
          Collections.sort(readers, taskComarator);
          reader = readers.peekFirst();
        }
        if (reader != null) readersByChannels.put(channel, reader);
      }
    }
    if (reader == null)
      throw new IOException("No one reader is created!");
    if (reader.getChannelsCount() > MAX_IO_CHANNELS_COUNT)
      throw new IOException("Maximum count of reader channels exceeded");

    reader.registerChannel(channel);
  }

  private void startupIO() throws IOException
  {
    logger.info("Starting up IO tasks...");
    synchronized (readers) {
      readers.clear();
      for (int i=0; i<Server.READERS_COUNT; i++) {
        try {
          RequestReader reader = new RequestReader(this);
          readers.add(reader); reader.start();
        } catch (IOException init_exc) {
          readers.forEach(RequestReader::shutdown); readers.clear();
          throw new IOException("Failed to start readers", init_exc);
        }
      }
    }
    synchronized (writers) {
      writers.clear();
      for (int i=0; i<Server.WRITERS_COUNT; i++) {
        try {
          ResponseWriter writer = new ResponseWriter(this);
          writers.add(writer); writer.start();
        } catch (IOException init_exc) {
          synchronized (readers) { readers.forEach(RequestReader::shutdown); readers.clear(); }
          writers.forEach(ResponseWriter::shutdown); writers.clear();
          throw new IOException("Failed to start writers", init_exc);
        }
      }
    }
    logger.info("IO tasks started");
  }

  private void shutdownIO()
  {
    synchronized (readers) { readers.forEach(RequestReader::shutdown); }
    synchronized (writers) { writers.forEach(ResponseWriter::shutdown); }
  }

}
