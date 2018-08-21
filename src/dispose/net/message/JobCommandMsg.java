package dispose.net.message;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

import dispose.net.links.LinkBrokenException;
import dispose.net.supervisor.DeadJobException;
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
    KILL,
    COMPLETE
  }
  
  
  public JobCommandMsg(UUID jid, Command cmd)
  {
    this.jid = jid;
    this.cmd = cmd;
  }

  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws MessageFailureException
  {
    Exception exc;
    Function<Job, Exception> jfunc;
    
    switch (cmd) {
      case START:
        jfunc = (Job job) -> {
          Exception exc2 = null;
          try {
            job.materialize();
            job.start();
          } catch (LinkBrokenException | ResourceUnderrunException e) {
            exc2 = e;
          }
          return exc2;
        };
        break;
      case COMPLETE:
        jfunc = (Job job) -> {
          job.completed();
          return null;
        };
        break;
      case KILL:
        jfunc = (Job job) -> {
          job.kill();
          return null;
        };
        break;
      default:
        throw new MessageFailureException("unrecognized command");
    }
    
    try {
      Future<Exception> fexc = supervis.executeJobFunction(jid, jfunc);
      exc = fexc.get();
    } catch (InterruptedException | ExecutionException | DeadJobException e) {
      throw new MessageFailureException(e);
    }
    if (exc != null)
      throw new MessageFailureException(exc);
  }
}
