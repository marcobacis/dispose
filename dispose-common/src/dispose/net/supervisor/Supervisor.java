package dispose.net.supervisor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import dispose.net.common.Config;

public class Supervisor implements Runnable
{
  private Set<NodeProxy> nodes;
  
  
  public Supervisor()
  {
    nodes = new HashSet<>();
  }

  
  @Override
  public void run()
  {
    while (true) {
      try {
        NodeProxy nm = NodeProxy.connectNodeMonitor(this, Config.nodeCtrlPort);
        registerNode(nm);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }
  }
  
  
  synchronized public void registerNode(NodeProxy nm)
  {
    nodes.add(nm);
  }
  
  
  synchronized protected void removeNode(NodeProxy nm)
  {
    nodes.remove(nm);
  }
  
  
  synchronized public Set<NodeProxy> getNodes()
  {
    return Collections.unmodifiableSet(new HashSet<>(nodes));
  }
}