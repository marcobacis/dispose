package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.CtrlMessage;
import dispose.net.node.Node;

public class ChkpRequestMsg extends CtrlMessage
{

  private static final long serialVersionUID = -6339978017034829113L;
  private UUID uuid = UUID.randomUUID();
  
  private int chkpId;
  
  public ChkpRequestMsg(int id)
  {
    this.chkpId = id;
  }
  
  public int getID()
  {
    return chkpId;
  }
  
  @Override
  public UUID getUUID()
  {
    return uuid;
  }
  
  @Override
  public void executeOnNode(Node node)
  {
    node.injectIntoSource(this);
  }
  

}
