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
import java.util.Iterator;

/**
 * Class extends base abstract IOTask class. Implements non-blocking calls reading.
 */
public class RequestReader extends IOTask
{
  final static Logger logger = LogManager.getLogger(RequestReader.class);

  private ByteBuffer readBuffer = ByteBuffer.allocate(CallStream.MAX_PACKET_SIZE);

  RequestReader(Server server) throws IOException {
    super(server);
  }

  void registerChannel(SocketChannel channel) throws IOException // Called from Server (so make it package private)
  {
    try {
      super.registerChannel(channel, SelectionKey.OP_READ);
    } catch (IOException exc) {
      throw new IOException("Failed to register channel for reading", exc);
    }
  }

  private void read(SelectionKey key) throws IOException, ClassNotFoundException
  {
    readBuffer.clear();
    SocketChannel channel = (SocketChannel) key.channel();
    int bytesRead = channel.read(readBuffer);
    if (logger.isTraceEnabled()) logger.trace("Received " + bytesRead + " bytes");
    if (bytesRead == -1) { // Remote entity shutdown the socket cleanly
      key.cancel(); server.closeChannel(channel);
      return;
    }
    CallBuffer callBuffer = (CallBuffer) key.attachment();
    if (callBuffer == null) {
      callBuffer = new CallBuffer(); key.attach(callBuffer);
    }
    readBuffer.flip();
    while (readBuffer.remaining() > 0) {
      Call call = callBuffer.readCall(readBuffer);
      if (call != null) // packet completed => return Call
        server.handleCall(call, channel);
    }
  }

  protected void process(SelectionKey key) {
    try {
      try {
        if (key.isReadable()) read(key);
        else throw new IOException("Unexpected channel state on read");
      } catch (Exception exc) {
        throw new IOException("Read failed", exc);
      }
    } catch (IOException exc) {
      SocketChannel channel = (SocketChannel) key.channel();
      if (channel != null && channel.isConnected()) server.handleError(exc, channel);
      key.cancel();
    }
  }

  protected void wakeup() {
    selector.wakeup();
  }

  public void run() {
    started = true;
    while (started) {
      try {
        selector.select();
      } catch (ClosedSelectorException closed_exc) {
        logger.error("Reader selector was closed: " + selector, closed_exc);
        break;
      } catch (IOException io_exc) {
        logger.error("Reader selector error: " + selector, io_exc);
        break;
      }
      processPendingRegistrations();

      Iterator iterator = selector.selectedKeys().iterator();
      while (iterator.hasNext()) {
        SelectionKey key = (SelectionKey) iterator.next(); iterator.remove();
        if (key.isValid()) process(key);
        else logger.warn("Reader selector key was cancelled, channel or selector was closed. Key=" + key);
      }
    }

    try {
      if (selector != null) selector.close();
    } catch (IOException close_exc) {
      logger.error("Failed to close reader selector. Details: " + close_exc);
    }
  }

  private enum BufferState { SEQNUM, SIZE, DATA }
  private class CallBuffer
  {
    private final ByteBuffer intBuffer = ByteBuffer.allocate(4);
    private BufferState state = BufferState.SEQNUM;
    private ByteBuffer dataBuffer = null;
    private Integer seqNum = null;
    private Integer size = null;

    private Integer readInt(ByteBuffer buffer) throws IOException {
      if (buffer.remaining() <= intBuffer.remaining()) intBuffer.put(buffer);
      else while (intBuffer.hasRemaining()) intBuffer.put(buffer.get());
      if (intBuffer.position() != intBuffer.capacity()) return null;
      intBuffer.flip();
      int result = intBuffer.getInt();
      intBuffer.clear();
      return result;
    }

    public Call readCall(ByteBuffer buffer) throws IOException, ClassNotFoundException {
      switch (state) {
        case SEQNUM:
          seqNum = readInt(buffer);
          if (seqNum == null) return null;
          state = BufferState.SIZE;
          // break skipped, need to read size
        case SIZE:
          size = readInt(buffer);
          if (size == null) return null;
          if (size < CallStream.MIN_PACKET_SIZE || size > CallStream.MAX_PACKET_SIZE)
            throw new IOException("Illegal received packet size=" + size);
          state = BufferState.DATA;
          dataBuffer = ByteBuffer.allocate(size);
          // break skipped, need to read the rest of buffer into dataBuffer
        case DATA:
          if (buffer.remaining() <= dataBuffer.remaining()) dataBuffer.put(buffer);
          else while (dataBuffer.hasRemaining()) dataBuffer.put(buffer.get());
          if (dataBuffer.position() == dataBuffer.capacity()) {
            state = BufferState.SEQNUM;
            dataBuffer.rewind();
            return CallStream.makeCall(dataBuffer.array(), seqNum); // call was received
          }
      }
      return null; // call wasn't received yet
    }
  }

}
