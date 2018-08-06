package dispose.net.supervisor;

import java.io.IOException;

import dispose.net.links.Link;
import dispose.net.links.SocketLink;

public class NodeMonitor implements Runnable
{
  Supervisor owner;
  Link link;
  
  
  public NodeMonitor(Supervisor owner, Link link)
  {
    this.owner = owner;
    this.link = link;
  }
  
  
  public static NodeMonitor connectNodeMonitor(Supervisor owner, int port) throws IOException
  {
    SocketLink tlink = SocketLink.connectFrom(port);
    NodeMonitor nm = new NodeMonitor(owner, tlink);
    Thread thd = new Thread(nm);
    thd.setName("node-monitor-" + Integer.toHexString(nm.nodeID()));
    thd.start();
    return nm;
  }
  
  
  @Override
  public void run()
  {
    while (true) {
      try {
        link.recvMsg(0);
      } catch (ClassNotFoundException | IOException e) {
        System.out.println("Node ID " + Integer.toHexString(nodeID()) + " down");
        e.printStackTrace();
        break;
      }
    }
    owner.removeNode(this);
  }

  
  public int nodeID()
  {
    return this.hashCode();
  }
}
