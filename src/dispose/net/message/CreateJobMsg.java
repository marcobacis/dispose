package dispose.net.message;

import java.util.UUID;

import dispose.client.ClientDag;
import dispose.net.supervisor.Job;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class CreateJobMsg extends CtrlMessage
{
  private static final long serialVersionUID = 3594328279621054155L;
  private ClientDag dag;
  private UUID jid;
  
  
  public CreateJobMsg(UUID jid, ClientDag dag)
  {
    this.dag = dag;
    this.jid = jid;
  }

  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws MessageFailureException
  {
    try {
      Job newjob = Job.jobFromClientDag(jid, dag, supervis, nodem);
      supervis.createJob(newjob);
    } catch (Exception e) {
      throw new MessageFailureException(e);
    }
  }
}
