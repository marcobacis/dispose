package dispose.net.message;

import java.util.UUID;

import dispose.client.ClientDag;
import dispose.log.DisposeLog;
import dispose.net.supervisor.Job;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class InstantiateDagMsg extends CtrlMessage
{
  private static final long serialVersionUID = 3594328279621054155L;
  private ClientDag dag;
  
  
  public InstantiateDagMsg(ClientDag dag)
  {
    this.dag = dag;
  }

  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws Exception
  {
    instantiateDag(supervis, nodem);
  }
  
  
  private void instantiateDag(Supervisor supervis, NodeProxy nodem) throws Exception
  {
    DisposeLog.debug(this, "InstantiateDag!!");
    DisposeLog.debug(this, dag.toString());
    
    Job newjob = Job.jobFromClientDag(UUID.randomUUID(), dag, supervis, nodem);
    supervis.createJob(newjob);
    newjob.materialize();
    
    DisposeLog.info(this, "dag instantiated!");
  }
}
