package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.Message;
import dispose.net.node.OperatorCheckpoint;

public class ChkpResponseMsg extends Message
{

  private static final long serialVersionUID = -1987640770334245047L;

  private UUID uuid = UUID.randomUUID();
  
  private int chkpId;
  private OperatorCheckpoint checkpoint;
  
  public ChkpResponseMsg(OperatorCheckpoint checkpoint)
  {
    this.chkpId = checkpoint.getID();
    this.checkpoint = checkpoint;
  }
  
  public int getID()
  {
    return chkpId;
  }
  
  public OperatorCheckpoint getCheckpoint()
  {
    return this.checkpoint;
  }
  
  @Override
  public UUID getUUID()
  {
    return uuid;
  }
  
}
