package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.CtrlMessage;
import dispose.net.node.Node;

public class ChkpRequestMsg extends CtrlMessage
{
  private static final long serialVersionUID = -6339978017034829113L;
  private UUID chkpId;
  
  
  public ChkpRequestMsg(UUID id)
  {
    this.chkpId = id;
  }
  
  
  public UUID getCheckpointID()
  {
    return chkpId;
  }
  
  
  @Override
  public void executeOnNode(Node node)
  {
    node.injectIntoSource(this);
  }
}
