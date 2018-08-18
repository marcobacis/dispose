package dispose.net.node.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.message.Message;
import dispose.net.message.chkp.ChkpRequestMsg;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.datasinks.DataSink;

public class SinkThread extends ComputeThread implements MonitoredLink.Delegate
{
  private DataSink dataSink;
  private List<MonitoredLink> inStreams = new ArrayList<>();
  private AtomicBoolean running = new AtomicBoolean(true);
  
  
  public SinkThread(Node owner, DataSink dataSink)
  {
    super(owner);
    this.dataSink = dataSink;
    this.opID = dataSink.getID();
  }
  
  
  @Override
  public void setInputLink(Link inputLink, int fromId) throws Exception
  {
    MonitoredLink monlink = new MonitoredLink(inputLink, this);
    inStreams.add(monlink);
  }


  @Override
  public void setOutputLink(Link outputLink, int toId) throws Exception
  {
    throw new Exception("this is a data --> SINK <--");
  }


  @Override
  public void pause()
  {
    running.set(false); 
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
  }

  @Override
  public void messageReceived(Message msg) throws Exception
  {
    if(msg instanceof DataAtom) {
      DataAtom da = (DataAtom)msg;
      dataSink.processAtom(da);
    } else if(msg instanceof ChkpRequestMsg) {
      //TODO handle checkpoint end
      ChkpRequestMsg chkp = (ChkpRequestMsg) msg;
      DisposeLog.debug(SinkThread.class, "Completed checkpoint " + chkp.getID());
    }
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    DisposeLog.error(this, "link is down");
  }

}
