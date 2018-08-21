
package dispose.net.node.threads;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.types.EndData;
import dispose.net.links.Link;
import dispose.net.links.LinkBrokenException;
import dispose.net.links.MonitoredLink.Delegate;
import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.datasources.DataSource;


public class SourceThread extends ComputeThread
{
  private DataSource dataSource;
  OperatorBroadcast outLink = new OperatorBroadcast();

  private AtomicBoolean running = new AtomicBoolean(false);
  private AtomicBoolean paused = new AtomicBoolean(false);
  private LinkedBlockingQueue<Message> injectQueue = new LinkedBlockingQueue<>();
  private UUID jid;

  public SourceThread(Node owner, UUID jid, DataSource dataSource)
  {
    super(owner);
    this.jid = jid;
    this.dataSource = dataSource;
    this.opID = dataSource.getID();
  }


  @Override
  public void addInputLink(Link inputLink, int fromId) throws ClosedEndException
  {
    throw new ClosedEndException("this is a data --> SOURCE <--");
  }


  @Override
  public void addOutputLink(Link outputLink, int toId) throws ClosedEndException
  {
    outLink.setOutputLink(outputLink, toId, new SourceDelegate());
  }


  private class SourceDelegate implements Delegate
  {

    @Override
    public void messageReceived(Message msg) throws MessageFailureException
    {
    }


    @Override
    public void linkIsBroken(Exception e)
    {
      stop();
    }

  }


  @Override
  public void pause()
  {
    paused.set(true);
  }

  public void resume()
  {
    paused.set(false);
    synchronized (paused) {
      paused.notify();
    }
  }

  @Override
  public void start()
  {
    running.set(true);
    Thread thd = new Thread(() -> mainLoop());
    thd.setName("data-source-" + Integer.toString(getID()));
    thd.start();
  }

  @Override
  public void stop()
  {
    running.set(false);

    outLink.close();

    dataSource.end();
  }

  private void mainLoop()
  {
    try {
      dataSource.setUp();

      while (true) {
        if (running.get() == false)
          return;

        if (paused.get()) {
          synchronized (paused) {
            while (paused.get()) {
              try {
                paused.wait();
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }

        Message d = injectQueue.poll();
        if (d == null)
          d = dataSource.nextAtom();
        
        outLink.sendMsg(d);
        
        if (d instanceof EndData) {
          pause();
        } 
      }
    } catch (LinkBrokenException e) {
      DisposeLog.error(this, "oh oh we've lost the link \\OwO/; exc = ", e);
    }
  }


  public void injectMessage(Message toInject)
  {
    injectQueue.offer(toInject);
  }

}
