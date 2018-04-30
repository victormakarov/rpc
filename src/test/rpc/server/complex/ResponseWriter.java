package test.rpc.server.complex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import test.rpc.Call;
import test.rpc.CallStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Class extends base abstract IOTask class. Implements non-blocking packets writing.
 */
public class ResponseWriter extends IOTask
{
  final static Logger logger = LogManager.getLogger(ResponseWriter.class);

  ResponseWriter(Server server) throws IOException {
    super(server);
  }

  void registerChannel(SocketChannel channel) throws IOException // Called from Server (so make it package private)
  {
    try {
      super.registerChannel(channel, SelectionKey.OP_WRITE);
    } catch (IOException exc) {
      throw new IOException("Failed to register channel for writing", exc);
    }
  }

  private class CallBuffer
  {
    private final Call call;
    private final ByteBuffer dataBuffer;

    public CallBuffer(Call call) throws IOException {
      if (call == null || call.getResponse() == null)
        throw new IOException("Call to send is null");

      byte[] packetData = CallStream.makeResponse(call.getResponse());
      int packetSize = packetData.length + 8;
      if (packetSize < CallStream.MIN_PACKET_SIZE || packetSize > CallStream.MAX_PACKET_SIZE)
        throw new IOException("Illegal serialized packet size=" + packetSize);

      this.call = call;
      dataBuffer = ByteBuffer.allocate(packetSize);
      dataBuffer.putInt(call.getSeqNum());
      dataBuffer.putInt(packetData.length);
      dataBuffer.put(packetData);
      dataBuffer.rewind();
    }

    public ByteBuffer getBuffer() {
      return dataBuffer;
    }
    public Call getCall() {
      return call;
    }
  }

  private final HashMap<SelectionKey, LinkedList<CallBuffer>> pendingPackets = new HashMap<>();

  public void send(Call call, SocketChannel channel) throws IOException
  {
    if (!started)
      throw new IOException("Writer is not started");
    CallBuffer buffer = new CallBuffer(call);
    synchronized (pendingPackets) {
      SelectionKey key = channel.keyFor(selector);
      if (key == null)
        throw new IOException("Channel is not registered in this writer");
      LinkedList<CallBuffer> queue = pendingPackets.get(key);
      if (queue == null) {
        queue = new LinkedList<>();
        pendingPackets.put(key, queue);
      }
      queue.addLast(buffer);
    }
    if (logger.isTraceEnabled()) logger.trace("Call seqNum=" + call.getSeqNum() + " put into writeQueue");
    wakeup();
  }

  private void write(SelectionKey key) throws IOException
  {
    synchronized (pendingPackets) {
      LinkedList<CallBuffer> queue = pendingPackets.get(key);
      if (queue == null) return;
      while (!queue.isEmpty()) {
        SocketChannel channel = (SocketChannel) key.channel();
        CallBuffer callBuffer = queue.peekFirst();
        final Call call = callBuffer.getCall();
        if (logger.isTraceEnabled()) logger.trace("Call seqNum=" + call.getSeqNum() + " is being written");
        ByteBuffer buffer = callBuffer.getBuffer();
        channel.write(buffer);
        if (buffer.remaining() > 0) return; // not all data was sent => continue wait & write
        queue.removeFirst();
      }
      pendingPackets.remove(key);
    }
  }

  private void cleanup(SelectionKey key) {
    synchronized (pendingPackets) {
      pendingPackets.remove(key);
    }
  }

  protected void wakeup() {
    selector.wakeup();
    synchronized (pendingPackets) {
      pendingPackets.notify();
    }
  }

  protected void process(SelectionKey key) {
    try {
      try {
        if (key.isWritable()) write(key);
        else throw new IOException("Unexpected channel state on write");
      } catch (IOException exc) {
        throw new IOException("Write failed", exc);
      }
    } catch (IOException exc) {
      SocketChannel channel = (SocketChannel) key.channel();
      if (channel != null && channel.isConnected()) server.handleError(exc, channel);
      cleanup(key); // clenup pending data (if any)
      key.cancel();
    }
  }

  public void run()
  {
    started = true;
    while (started) {
      synchronized (pendingPackets) {
        while (started && pendingPackets.isEmpty() && noPendingRegistration()) {
          try {pendingPackets.wait(Server.IO_TIMEOUT); }
          catch (InterruptedException e) { logger.trace(e); }
        }
      }
      if (!started) break;

      try {
        selector.select();
      } catch (ClosedSelectorException closed_exc) {
        logger.error("Writer selector was closed: " + selector, closed_exc);
        break;
      } catch (IOException io_exc) {
        logger.error("Writer selector error: " + selector, io_exc);
        break;
      }
      processPendingRegistrations();

      Iterator iterator = selector.selectedKeys().iterator();
      while (iterator.hasNext()) {
        SelectionKey key = (SelectionKey) iterator.next();
        iterator.remove();
        if (key.isValid()) process(key);
        else logger.warn("Writer selector key was cancelled, channel or selector was closed. Key=" + key);
      }
    }

    for (SelectionKey key : selector.keys()) {
      cleanup(key); // clenup pending data (if any)
      key.cancel(); // deregister channel
    }

    try { selector.close(); } catch (IOException close_exc) {
      logger.error("Failed to close writer selector. Details: " + close_exc);
    }
  }

}
