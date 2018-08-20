package dispose.net.supervisor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import dispose.log.DisposeLog;
import dispose.net.common.Config;

public class Supervisor implements Runnable
{
  private Set<NodeProxy> nodes = new HashSet<>();
  private Map<UUID, Job> currentJobs = new HashMap<>();

  
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
    
    for (Job job: currentJobs.values()) {
      job.nodeHasDied(nm);
    }
  }
  
  
  synchronized public Set<NodeProxy> getNodes()
  {
    return Collections.unmodifiableSet(new HashSet<>(nodes));
  }
  
  
  synchronized public void createJob(Job j)
  {
    DisposeLog.info(this, "created job ", j.getID());
    currentJobs.put(j.getID(), j);
  }
  
  
  synchronized public Job getJob(UUID jobid)
  {
    return currentJobs.get(jobid);
  }
}
