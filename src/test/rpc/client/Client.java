package test.rpc.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import test.rpc.Call;
import test.rpc.CallStream;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class implements simple interface for RPC-processing.
 *
 * Client, running in single thread, creates connection to Server
 * and receives responses for calls initiated by call() method.
 *
 * Method call() can call remote Service's methods on Server via binary TCP/IP-based protocol.
 * Returns remote method's execution result, including possible processing Exception (via ExecutionException).
 * Null & Void result is also supported.
 *
 * Blocks until call processing will be done or io-error be occured on server connection.
 * Method is thread-safe and can be called simultaneously by a number of caller's threads.
 * Throws IllegalStateException for calls made after the client shutdown.
 *
 * Date: 27.04.2018
 * Time: 23:38
 */
public class Client extends Thread
{
  private final Logger logger = LogManager.getLogger(this.getClass());

  /**
   * Creates and Client
   *
   * @param host Server host
   * @param port Server port
   */
  public Client(String host, int port) {
    this.host = host; this.port = port;
  }

  /**
   * Method calls remote Service's method on Server.
   * Returns remote method's execution result, including possible processing Exception (via ExecutionException).
   * Null & Void results are also supported.
   *
   * Blocks until call processing will be done or io-error be occured on server connection.
   * Method is thread-safe and can be called simultaneously by a number of caller's threads.
   *
   * @param service remote Service name to call
   * @param method remote Service's method to call
   * @param params remote method's parameters to call with
   *
   * @return remote method results:
   * null       - if remote method returned null
   * Object     - if remote method returned not null Object
   * Void.TYPE  - if remote method return type is void
   *
   * @throws ExecutionException remote method's exception or exception on response receive
   * @throws IOException if communication exception on server connection occured
   * @throws InterruptedException when calling thread interrupted and/or due to client shuttdown
   * @throws IllegalStateException when call is made after the client shutdown
   */
  public Object call(String service, String method, Object[] params)
      throws ExecutionException, IOException, InterruptedException, IllegalStateException
  {
    if (!started)
      throw new IllegalStateException("Client is not started or exiting");

    int seqNum = getNextSeqNum();
    logger.info("Call seqNum=" + seqNum + " service=" + service + ", method=" + method +
                " with params: " + Arrays.toString(params));

    Call call = new Call(seqNum, service, method, params);
    Future future = call(call);
    if (!future.isDone()) {
      future.get();
    }

    final Exception e = call.getExc();
    if (e != null)
      throw new ExecutionException("Call processing exception", e);

    final Call.Response response = call.getResponse();
    if (response == null)
      throw new ExecutionException("Call seqNum=" + seqNum + " response is null", new NullPointerException());

    final Object result = response.getResult();
    final Exception exception = response.getExc();
    final String msg = (result != null) ?  result.toString() : (exception != null) ? exception.toString() : null;

    logger.info("Call seqNum=" + seqNum + ". Result received: " + msg);

    if (exception != null)
      throw new ExecutionException("Service method threw exception", exception);

    return result;
  }

  /**
   * Starts up Client, creates connection to remote Server
   *
   * @throws IOException when connection to Server with specified host:port failed to be established
   */
  public void startup() throws IOException
  {
    try {
      socket = new Socket(InetAddress.getByName(host), port);
      is = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      os = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    } catch (IOException e) {
      logger.error("Failed to start Client on " + host + ":" + port);
      throw e;
    }

    super.start();
    while (!started) {
      try { synchronized (this) { this.wait(10); }}
      catch (InterruptedException e) { logger.trace(e); }
    }
  }

  /**
   * Shutdowns client and all pending calls
   */
  public void shutdown()
  {
    logger.info("Shutting down client...");

    this.started = false;
    cancelPendingCalls();
    super.interrupt();

    try { os.close(); } catch (IOException e) {
      logger.error("Failed to close os=" + os);
    }
    try { is.close(); } catch (IOException e) {
      logger.error("Failed to close is=" + is);
    }
    try { if (this.socket != null) socket.close(); } catch (IOException e) {
      logger.error("Failed to close socket=" + socket);
    }

    try { sleep(100); } catch (InterruptedException e) {
      logger.debug(e);
    }
  }

  @Override
  public void run() // reads responses
  {
    if (started) return;
    started = true;
    while (started)
    {
      Exception exc = null;
      Call.Response response = null;
      Integer seqNum = null;
      try {
        seqNum = is.readInt();
        response = CallStream.readResponse(is, seqNum);
      } catch (EOFException e) {
        logger.info((!started) ? "Connection closed" : "Server closed connection");
        break;
      } catch (Exception e) {
        if (!started || seqNum == null) {
          logger.warn((!started) ? "Exiting" : "Connection to server is lost. Details: " + e.getMessage());
          break;
        }
        exc = e; // to throw Exception for call
      }

      final FutureCall future;
      synchronized (calls) {
        future = calls.remove(seqNum);
      }

      if (future == null) {
        logger.warn("Call for seqNum=" + seqNum + " not found");
      } else {
        if (exc != null) future.setExc(exc);
        else future.setResponse(response);
      }
    }
    cancelPendingCalls();
    started = false;
  }

  synchronized private int getNextSeqNum() {
    return ++currSeq;
  }

  private Future call(Call call) throws InterruptedException, IOException
  {
    final Integer seqNum = call.getSeqNum();
    final Call.Request request = call.getRequest();
    final FutureCall future = new FutureCall(call);

    synchronized (calls) {
      calls.put(seqNum, future);
      os.writeInt(seqNum);
      CallStream.writePacket(os, request);
    }
    return future;
  }

  private class FutureCall implements Future
  {
    private volatile boolean cancelled = false;
    private volatile boolean done = false;

    private final Call call;
    public FutureCall(Call call) {
      this.call = call;
    }

    public void setResponse(Call.Response response) {
      synchronized (this) {
        call.setResponse(response);
        done = true;
        notify();
      }
    }
    public void setExc(Exception exc) {
      synchronized (this) {
        call.setExc(exc);
        done = true;
        notify();
      }
    }

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled = true;
      if (mayInterruptIfRunning) {
        setExc(new InterruptedException("Call was cancelled"));
      }
      return true;
    }
    @Override public boolean isCancelled() {
      return cancelled;
    }
    @Override public boolean isDone() {
      return done;
    }

    @Override public Object get() throws InterruptedException, ExecutionException
    {
      while (started && !(isCancelled() || isDone())) {
        synchronized (this) {
          this.wait();
        }
      }
      return this.call;
    }
    @Override public Object get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
      synchronized (this) {
        this.wait(unit.toMillis(timeout));
      }
      if (isDone()) return this.call;
      else throw new TimeoutException("Call is not finished yet");
    }
  }

  private void cancelPendingCalls()
  {
    synchronized (calls) {     // finalize processing calls
      Iterator<Map.Entry<Integer, FutureCall>> i = calls.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<Integer, FutureCall> entry = i.next();
        logger.debug("Finalizing Call for seqNum=" + entry.getKey());
        entry.getValue().cancel(true);
        i.remove();
      }
      calls.notifyAll();
    }
  }

  private final String host;
  private final int port;

  private Socket socket;
  private DataInputStream is;
  private DataOutputStream os;

  private int currSeq = 0;
  private volatile boolean started = false;
  private final HashMap<Integer, FutureCall> calls = new HashMap<>(100);
}
