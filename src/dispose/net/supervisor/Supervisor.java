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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.node.checkpoint.Checkpoint;

public class Supervisor implements Runnable
{
  private Set<NodeProxy> nodes = new HashSet<>();
  private Map<UUID, Job> currentJobs = new HashMap<>();
  private Map<UUID, ScheduledExecutorService> jobSerialQueues = new HashMap<>(); 
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
    jobSerialQueues.put(j.getID(), Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r)
      {
        Thread thd = new Thread(r);
        thd.setName("job-" + j.getID().toString());
        return thd;
      }
    }));
  }
  
  
  synchronized protected void removeJob(UUID jid)
  {
    ExecutorService eq = jobSerialQueues.get(jid);
    eq.shutdown();
    currentJobs.remove(jid);
    jobSerialQueues.remove(jid);
  }
  
  
  synchronized public <T> Future<T> executeJobFunction(
    UUID jobid, 
    Function<Job, T> qexc) throws DeadJobException
  {
    return executeJobFunctionAfterDelay(jobid, qexc, 0);
  }
  
  
  synchronized public <T> ScheduledFuture<T> executeJobFunctionAfterDelay(
    UUID jobid, 
    Function<Job, T> qexc, 
    long delayms) throws DeadJobException
  {
    Job j = currentJobs.get(jobid);
    ScheduledExecutorService q = jobSerialQueues.get(jobid);
    if (j == null || q == null)
      throw new DeadJobException();
    return q.schedule(() -> {
      return qexc.apply(j);
    }, delayms, TimeUnit.MILLISECONDS);
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
