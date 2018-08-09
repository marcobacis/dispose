package dispose.net.supervisor;

import java.io.IOException;

import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.links.SocketLink;
import dispose.net.message.CtrlMessage;
import dispose.net.message.LogMsg;

public class NodeMonitor implements MonitoredLink.Delegate
{
  private Supervisor owner;
  private MonitoredLink link;
  
  
  public NodeMonitor(Supervisor owner, Link link) throws IOException
  {
    this.owner = owner;
    this.link = MonitoredLink.asyncMonitorLink(link, this);
    this.link.sendMsg(new LogMsg("supervisor", "Node ID = " + Integer.toHexString(nodeID())));
  }
  
  
  public static NodeMonitor connectNodeMonitor(Supervisor owner, int port) throws IOException
  {
    SocketLink tlink = SocketLink.connectFrom(port);
    NodeMonitor nm = new NodeMonitor(owner, tlink);
    return nm;
  }

  
  public int nodeID()
  {
    return this.hashCode();
  }


  @Override
  public void messageReceived(CtrlMessage msg)
  {
    System.out.println("message received");
    return;
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    e.printStackTrace();
    owner.removeNode(this);
    link = null;
  }
  
  
  public MonitoredLink getLink()
  {
    return link;
  }
}
