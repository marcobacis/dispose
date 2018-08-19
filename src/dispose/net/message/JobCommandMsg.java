package dispose.net.message;

import java.util.UUID;

import dispose.net.links.LinkBrokenException;
import dispose.net.supervisor.Job;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.ResourceUnderrunException;
import dispose.net.supervisor.Supervisor;

public class JobCommandMsg extends CtrlMessage
{
  private static final long serialVersionUID = 8921646248221518208L;
  private UUID jid;
  private Command cmd;
  
  
  public enum Command
  {
    START,
    STOP
  }
  
  
  public JobCommandMsg(UUID jid, Command cmd)
  {
    this.jid = jid;
    this.cmd = cmd;
  }

  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws MessageFailureException
  {
    Job job = supervis.getJob(jid);
    
    switch (cmd) {
      case START:
        try {
          job.materialize();
          job.start();
        } catch (LinkBrokenException | ResourceUnderrunException e) {
          throw new MessageFailureException(e);
        }
        break;
      case STOP:
        break;
    }
  }
}
