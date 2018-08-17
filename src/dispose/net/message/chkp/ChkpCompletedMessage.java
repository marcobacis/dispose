package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.Message;

public class ChkpCompletedMessage extends Message
{

  private static final long serialVersionUID = -6339978017034829113L;
  private UUID uuid = UUID.randomUUID();
  
  private int chkpId;
  
  public ChkpCompletedMessage(int id)
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

}
