package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.CtrlMessage;
import dispose.net.message.MessageFailureException;
import dispose.net.node.OperatorCheckpoint;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class ChkpResponseMsg extends CtrlMessage
{
  private static final long serialVersionUID = -1987640770334245047L;
  private UUID chkpId;
  private OperatorCheckpoint checkpoint;
  
  
  public ChkpResponseMsg(OperatorCheckpoint checkpoint)
  {
    this.chkpId = checkpoint.getID();
    this.checkpoint = checkpoint;
  }
  
  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws MessageFailureException
  {
    supervis.receiveCheckpointPart(chkpId, checkpoint);
  }
}
