package test.rpc.server.complex;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

/**
 * Abstract class is a base for Reader and Writer classes.
 * Agregates one Selector instance to operate it in own Thread.
 * Calls abstract process(key) method on channel activity
 */
public abstract class IOTask extends Thread
{
  protected final Server server;
  protected final Selector selector;
  protected boolean started = false;

  IOTask(Server server) throws IOException {
    this.server = server;
    this.selector = Selector.open();
  }

  private final LinkedList<Registration> pendingRegistrations = new LinkedList<>();

  private enum RegistrationType { REGISTER, UNREGISTER }

  private class Registration {
    private final RegistrationType type;
    private final SocketChannel channel;
    private final int registerOptions;
    private boolean finished = false;
    private IOException exc = null;

    public Registration(SocketChannel channel) {
      this.channel = channel; this.registerOptions = 0;
      this.type = RegistrationType.UNREGISTER;
    }

    public Registration(SocketChannel channel, int options) {
      this.channel = channel; this.registerOptions = options;
      this.type = RegistrationType.REGISTER;
    }

    public synchronized void process() {
      if (type == RegistrationType.UNREGISTER) {
        SelectionKey key = channel.keyFor(selector);
        if (key != null) key.cancel();
      } else {
        try {
          channel.register(selector, registerOptions);
        } catch (IOException register_exc) {
          exc = register_exc;
        }
      }
      finished = true;
      this.notifyAll();
    }

    public boolean isFinished() {
      return finished;
    }

    public void checkRegistration() throws IOException {
      if (exc != null) throw exc;
    }
  }

  protected void registerChannel(SocketChannel channel, int ops) throws IOException {
    if (!started) return;
    final Registration registration = new Registration(channel, ops);
    synchronized (pendingRegistrations) {
      pendingRegistrations.addLast(registration);
    }
    synchronized (registration) {
      wakeup();
      while (!registration.isFinished()) {
        try {
          registration.wait(Server.IO_TIMEOUT);
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        }
      }
    }
    registration.checkRegistration();
  }

  protected void unregisterChannel(SocketChannel channel) {
    final Registration registration = new Registration(channel);
    synchronized (pendingRegistrations) {
      pendingRegistrations.addLast(registration);
    }
    wakeup();
  }

  protected void processPendingRegistrations() {
    synchronized (pendingRegistrations) {
      while (!pendingRegistrations.isEmpty()) {
        final Registration registration = pendingRegistrations.pollFirst();
        if (registration != null) registration.process();
      }
    }
  }

  protected boolean noPendingRegistration() {
    synchronized (pendingRegistrations) {
      return pendingRegistrations.isEmpty();
    }
  }

  public void shutdown() {
    started = false;
    wakeup();
  }

  public int getChannelsCount() {
    return selector.keys().size();
  }

  protected abstract void wakeup();
}
