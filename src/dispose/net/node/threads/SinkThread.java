package dispose.net.node.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.message.ChkpMessage;
import dispose.net.message.Message;
import dispose.net.node.ComputeThread;
import dispose.net.node.datasinks.DataSink;

public class SinkThread extends ComputeThread implements MonitoredLink.Delegate
{
  private DataSink dataSink;
  private List<MonitoredLink> inStreams = new ArrayList<>();
  private AtomicBoolean running = new AtomicBoolean(true);
  
  
  public SinkThread(DataSink dataSink)
  {
    this.dataSink = dataSink;
    this.opID = dataSink.getID();
  }
  
  
  @Override
  public void addInput(Link inputLink) throws Exception
  {
    MonitoredLink monlink = new MonitoredLink(inputLink, this);
    inStreams.add(monlink);
  }


  @Override
  public void addOutput(Link outputLink) throws Exception
  {
    throw new Exception("this is a data --> SINK <--");
  }


  @Override
  public void pause()
  {
    running.set(false);
    for (MonitoredLink ml: inStreams) {
      ml.close();
    }
  }


  @Override
  public void start()
  {
    for (MonitoredLink ml: inStreams) {
      Thread thd = new Thread(() -> ml.monitorSynchronously());
      thd.setName("data-sink-" + Integer.toString(opID) + "-" + Integer.toHexString(ml.hashCode()));
      thd.start();
    }
  }


  @Override
  public void messageReceived(Message msg) throws Exception
  {
    if(msg instanceof DataAtom) {
      DataAtom da = (DataAtom)msg;
      dataSink.processAtom(da);
    } else if(msg instanceof ChkpMessage) {
      //TODO handle checkpoint end
      ChkpMessage chkp = (ChkpMessage) msg;
      DisposeLog.debug(SinkThread.class, "Completed checkpoint " + chkp.getID());
    }
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    DisposeLog.error(this, "link is down");
  }

}
