package dispose.net.message.chkp;

import java.util.UUID;

import dispose.log.DisposeLog;
import dispose.net.message.CtrlMessage;
import dispose.net.message.MessageFailureException;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class ChkpCompletedMessage extends CtrlMessage
{
  private static final long serialVersionUID = -6339978017034829113L;
  private UUID chkpId;
  
  
  public ChkpCompletedMessage(UUID id)
  {
    this.chkpId = id;
  }
  
  
  public UUID getCheckpointID()
  {
    return chkpId;
  }

  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws MessageFailureException
  {
    DisposeLog.info(this, "checkpoint request ", chkpId, " arrived at the sink");
  }
}
