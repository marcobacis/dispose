package dispose.net.supervisor;

import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.links.SocketLink;
import dispose.net.message.CtrlMessage;
import dispose.net.message.LogMsg;
import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;

public class NodeProxy implements MonitoredLink.Delegate
{
  private Supervisor owner;
  private MonitoredLink link;
  private String networkAddress;
  
  public static String LOCAL_NETWORK_ADDRESS = "127.0.0.1";
  
  
  public NodeProxy(Supervisor owner, Link link, String networkAddress) throws Exception
  {
    this.owner = owner;
    this.networkAddress = networkAddress;
    this.link = MonitoredLink.asyncMonitorLink(link, this, Config.heartbeatPeriod * 2);
    this.link.sendMsg(new LogMsg("supervisor", "Node ID = " + Integer.toHexString(nodeID())));
  }
  
  
  public static NodeProxy connectNodeMonitor(Supervisor owner, int port) throws Exception
  {
    SocketLink tlink = SocketLink.connectFrom(port);
    NodeProxy nm = new NodeProxy(owner, tlink, tlink.remoteHostAddress());
    return nm;
  }

  
  public int nodeID()
  {
    return this.hashCode();
  }


  @Override
  public void messageReceived(Message msg) throws MessageFailureException
  {
    CtrlMessage cmsg = (CtrlMessage)msg;
    cmsg.executeOnSupervisor(owner, this);
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    DisposeLog.critical(this, "node @ ", networkAddress, " unavailable; exc = ", e != null ? e : "timeout");
    owner.removeNode(this);
    link = null;
  }
  
  
  public MonitoredLink getLink()
  {
    return link;
  }
  
  
  public String getNetworkAddress()
  {
    return networkAddress;
  }
}
