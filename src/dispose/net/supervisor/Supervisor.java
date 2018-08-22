package dispose.net.supervisor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.node.checkpoint.Checkpoint;

public class Supervisor implements Runnable
{
  private Set<NodeProxy> nodes = new HashSet<>();
  private Map<UUID, Job> currentJobs = new HashMap<>();
  private Map<UUID, ExecutorService> jobSerialQueues = new HashMap<>(); 
  private Timer checkpointTimer;

  
  @Override
  public void run()
  {
    CheckpointTimerTask ckpTimTask = new CheckpointTimerTask();
    ckpTimTask.owner = this;
    checkpointTimer = new Timer("checkpoint-timer");
    checkpointTimer.schedule(ckpTimTask, 10, Config.checkpointPeriod);
    
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
      ExecutorService q = jobSerialQueues.get(job.getID());
      q.submit(() -> job.nodeHasDied(nm));
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
    jobSerialQueues.put(j.getID(), Executors.newSingleThreadExecutor());
  }
  
  
  synchronized protected void removeJob(UUID jid)
  {
    ExecutorService eq = jobSerialQueues.get(jid);
    eq.shutdown();
    currentJobs.remove(jid);
    jobSerialQueues.remove(jid);
  }
  
  
  synchronized public <T> Future<T> executeJobFunction(UUID jobid, Function<Job, T> qexc) throws DeadJobException
  {
    Job j = currentJobs.get(jobid);
    ExecutorService q = jobSerialQueues.get(jobid);
    if (j == null || q == null)
      throw new DeadJobException();
    return q.submit(() -> {
      return qexc.apply(j);
    });
  }
  
  
  private class CheckpointTimerTask extends TimerTask
  {
    Supervisor owner;
    
    @Override
    public void run()
    {
      owner.initiateCheckpoints();
    }
  }
  
  
  synchronized private void initiateCheckpoints()
  {
    for (Job job: currentJobs.values()) {
      ExecutorService q = jobSerialQueues.get(job.getID());
      q.submit(() -> job.requestCheckpoint());
    }
  }
  
  
  synchronized public void receiveCheckpointPart(UUID ckpId, Checkpoint ckpPart)
  {
    for (Job job: currentJobs.values()) {
      ExecutorService q = jobSerialQueues.get(job.getID());
      q.submit(() -> job.reclaimCheckpointPart(ckpId, ckpPart));
    }
  }
}
