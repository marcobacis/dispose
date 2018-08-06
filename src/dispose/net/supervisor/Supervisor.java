package dispose.net.supervisor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import dispose.net.common.Config;

public class Supervisor implements Runnable
{
  private Set<NodeMonitor> nodes;
  
  
  public Supervisor()
  {
    nodes = new HashSet<>();
  }

  @Override
  public void run()
  {
    while (true) {
      try {
        NodeMonitor nm = NodeMonitor.connectNodeMonitor(this, Config.nodeCtrlPort);
        registerNode(nm);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
    }
  }
  
  
  synchronized protected void registerNode(NodeMonitor nm)
  {
    nodes.add(nm);
  }
  
  
  synchronized protected void removeNode(NodeMonitor nm)
  {
    nodes.remove(nm);
  }
}
