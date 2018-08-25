
package dispose.net.node.threads;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.log.LogInfo;
import dispose.net.common.types.EndData;
import dispose.net.links.Link;
import dispose.net.links.LinkBrokenException;
import dispose.net.links.MonitoredLink.Delegate;
import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;
import dispose.net.message.chkp.ChkpRequestMsg;
import dispose.net.message.chkp.ChkpResponseMsg;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.checkpoint.Checkpoint;
import dispose.net.node.checkpoint.DataSourceCheckpoint;
import dispose.net.node.datasources.DataSource;


public class SourceThread extends ComputeThread implements Delegate, LogInfo
{
  private DataSource dataSource;
  DataMulticaster outLink = new DataMulticaster();

  private AtomicBoolean running = new AtomicBoolean(false);
  private AtomicBoolean paused = new AtomicBoolean(false);
  private LinkedBlockingQueue<Message> injectQueue = new LinkedBlockingQueue<>();

  
  public SourceThread(Node owner, UUID jid, DataSource dataSource)
  {
    super(owner, jid);
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
    outLink.addOutputLink(outputLink, toId, this);
  }


  @Override
  public void messageReceived(Message msg) throws MessageFailureException
  {
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    pause();
  }


  @Override
  public synchronized void pause()
  {
    paused.set(true);
  }

  
  public synchronized void resume()
  {
    paused.set(false);
    notify();
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
    dataSource.setUp();

    while (true) {
      if (running.get() == false)
        return;

      Message msg;
      
      synchronized (this) {
        if (paused.get()) {
          while (paused.get()) {
            try {
              wait();
            } catch (InterruptedException e) {
              return;
            }
          }
        }
  
        msg = injectQueue.poll();
        if (msg == null) {
          msg = dataSource.nextAtom();
        }
      }
      
      if(msg instanceof ChkpRequestMsg) {
        ChkpRequestMsg req = (ChkpRequestMsg) msg;
        DataSourceCheckpoint chkp = new DataSourceCheckpoint(req.getCheckpointID(), dataSource, injectQueue);
        owner.sendMsgToSupervisor(opID, new ChkpResponseMsg(chkp));
      }
      
      try {
        outLink.sendMsg(msg);
      } catch (LinkBrokenException e) {
        DisposeLog.error(this, "we've lost a link; exc = ", e, "; pausing");
        paused.set(true);
      }
      
      if (msg instanceof EndData) {
        paused.set(true);
      }
    }
  }


  public void injectMessage(Message toInject)
  {
    injectQueue.offer(toInject);
  }


  @Override
  public void reloadFromCheckpoint(Checkpoint chkp)
  {
    DisposeLog.info(this, "reloading source");
    DataSourceCheckpoint checkpoint = (DataSourceCheckpoint) chkp;
    
    synchronized (this) {
      dataSource = (DataSource) checkpoint.getComputeNode();
    }
    injectQueue = checkpoint.getInjectQueue();
    DisposeLog.info(this, "restored source");
  }


  @Override
  public String loggingName()
  {
    return "Source " + dataSource.getClass().getSimpleName() + " " + Integer.toString(dataSource.getID());
  }
 
}
