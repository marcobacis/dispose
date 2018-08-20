package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.Message;

public class ChkpCompletedMessage extends Message
{
  private static final long serialVersionUID = -6339978017034829113L;
  private UUID uuid = UUID.randomUUID();
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
  public UUID getUUID()
  {
    return uuid;
  }

}
