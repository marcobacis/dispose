package dispose.net.node.threads;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import dispose.log.DisposeLog;
import dispose.log.LogInfo;
import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.message.JobCommandMsg;
import dispose.net.message.JobCommandMsg.Command;
import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;
import dispose.net.message.chkp.ChkpCompletedMessage;
import dispose.net.message.chkp.ChkpRequestMsg;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.checkpoint.Checkpoint;
import dispose.net.node.datasinks.DataSink;

public class SinkThread extends ComputeThread implements LogInfo
{
  private DataSink dataSink;
  private Map<Integer, MonitoredLink> inStreams = new HashMap<>();
  private State state = State.SETUP;
  private DataAtom lastAtom;
  
  
  private enum State
  {
    SETUP, PAUSED, RUNNING, STOPPED
  }
  
  
  public SinkThread(Node owner, UUID jid, DataSink dataSink)
  {
    super(owner, jid);
    this.dataSink = dataSink;
    this.opID = dataSink.getID();
  }
  
  
  @Override
  public void addInputLink(Link inputLink, int fromId) throws ClosedEndException
  {
    if (inStreams.containsKey(fromId)) {
      MonitoredLink oldLink = inStreams.get(fromId);
      oldLink.close();
    }
    
    MonitoredLink monlink = MonitoredLink.asyncMonitorLink(inputLink, new Delegate(fromId), 0);
    inStreams.put(fromId, monlink);
  }


  @Override
  public void addOutputLink(Link outputLink, int toId) throws ClosedEndException
  {
    throw new ClosedEndException("this is a data --> SINK <--");
  }


  @Override
  public synchronized void pause()
  {
    if (state == State.RUNNING) {
      DisposeLog.debug(this, "Sink thread paused");
      state = State.PAUSED;
    }
  }
  
  
  @Override
  public synchronized void resume()
  {
    if (state == State.RUNNING)
      return;
    state = State.RUNNING;
  }

  
  @Override
  public synchronized void start()
  {
    if (state == State.RUNNING)
      return;
    dataSink.setUp();
    state = State.RUNNING;
  }

  
  @Override
  public synchronized void stop()
  {
    if (state == State.STOPPED)
      return;
    
    /* make sure all readers are unlocked, so that we don't leave behind
     * any zombie thread */
    state = State.STOPPED;
    
    for (MonitoredLink ml: inStreams.values()) {
      ml.close();
    }
    
    dataSink.end();
    DisposeLog.debug(this, "Sink thread stopped");
  }
  
  
  private class Delegate implements MonitoredLink.Delegate
  {
    int fromId;
    
    
    private Delegate(int fromId)
    {
      this.fromId = fromId;
    }
    
    
    @Override
    public void messageReceived(Message msg) throws MessageFailureException
    {
      SinkThread.this.messageReceived(msg, fromId);
    }

    
    @Override
    public void linkIsBroken(Exception e)
    {
      SinkThread.this.linkIsBroken(e);
    }
  }

  
  public synchronized void messageReceived(Message msg, int fromId)
  {
    if (state != State.RUNNING)
      return;
    
    if(msg instanceof EndData) {
      DisposeLog.debug(this, "End data received at the sink");
      JobCommandMsg endMsg = new JobCommandMsg(jid, Command.COMPLETE);
      owner.sendMsgToSupervisor(opID, endMsg);
      pause();
      
    } else if (msg instanceof DataAtom) {
      DataAtom da = (DataAtom)msg;
      lastAtom = da;
      dataSink.processAtom(da, fromId);
      
    } else if(msg instanceof ChkpRequestMsg) {
      ChkpRequestMsg chkp = (ChkpRequestMsg) msg;
      DisposeLog.debug(SinkThread.class, "Completed checkpoint ", chkp.getCheckpointID(), " last value ", lastAtom);
      owner.sendMsgToSupervisor(opID, new ChkpCompletedMessage(chkp.getCheckpointID()));
    }
  }


  public void linkIsBroken(Exception e)
  {
    DisposeLog.error(this, "link is down");
    pause();
  }


  @Override
  public void reloadFromCheckpoint(Checkpoint chkp)
  {
    // Do nothing, we don't need to do anything to restore the Sink
  }


  @Override
  public String loggingName()
  {
    return "Sink " + dataSink.getClass().getSimpleName() + " " + Integer.toString(dataSink.getID());
  }

}
