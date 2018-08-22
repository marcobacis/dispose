package dispose.net.node.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.message.JobCommandMsg;
import dispose.net.message.JobCommandMsg.Command;
import dispose.net.message.Message;
import dispose.net.message.chkp.ChkpCompletedMessage;
import dispose.net.message.chkp.ChkpRequestMsg;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.checkpoint.Checkpoint;
import dispose.net.node.datasinks.DataSink;

public class SinkThread extends ComputeThread implements MonitoredLink.Delegate
{
  private DataSink dataSink;
  private List<MonitoredLink> inStreams = new ArrayList<>();
  private AtomicBoolean running = new AtomicBoolean(true);
  
  public SinkThread(Node owner, UUID jid, DataSink dataSink)
  {
    super(owner, jid);
    this.dataSink = dataSink;
    this.opID = dataSink.getID();
  }
  
  
  @Override
  public void addInputLink(Link inputLink, int fromId) throws ClosedEndException
  {
    MonitoredLink monlink = new MonitoredLink(inputLink, this);
    inStreams.add(monlink);
  }


  @Override
  public void addOutputLink(Link outputLink, int toId) throws ClosedEndException
  {
    throw new ClosedEndException("this is a data --> SINK <--");
  }


  @Override
  public void pause()
  {
    running.set(false);
    DisposeLog.debug(this, "Sink thread paused");
  }
  
  @Override
  public void resume()
  {
    running.set(true);
  }

  @Override
  public void start()
  {
    for (MonitoredLink ml: inStreams) {
      Thread thd = new Thread(() -> ml.monitorSynchronously());
      thd.setName("data-sink-" + Integer.toString(opID) + "-" + Integer.toHexString(ml.hashCode()));
      thd.start();
    }
    
    dataSink.setUp();
  }

  @Override
  public void stop()
  {
    for (MonitoredLink ml: inStreams) {
      ml.close();
    }
    
    dataSink.end();
    
    DisposeLog.debug(this, "Sink thread stopped");
  }

  @Override
  public void messageReceived(Message msg)
  {
    if(msg instanceof EndData) {
      DisposeLog.debug(this, "End data received at the sink");
      JobCommandMsg endMsg = new JobCommandMsg(jid, Command.COMPLETE);
      owner.sendMsgToSupervisor(opID, endMsg);
      pause();
      
    } else if(msg instanceof DataAtom) {
      DataAtom da = (DataAtom)msg;
      dataSink.processAtom(da);
    } else if(msg instanceof ChkpRequestMsg) {
      ChkpRequestMsg chkp = (ChkpRequestMsg) msg;
      DisposeLog.debug(SinkThread.class, "Completed checkpoint ", chkp.getCheckpointID());
      owner.sendMsgToSupervisor(opID, new ChkpCompletedMessage(chkp.getCheckpointID()));
    }
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    DisposeLog.error(this, "link is down");
  }


  @Override
  public void reloadFromCheckpoint(Checkpoint chkp)
  {
    // Do nothing, we don't need to do anything to restore the Sink
  }

}
