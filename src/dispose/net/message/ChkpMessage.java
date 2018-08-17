package dispose.net.message;

import java.util.UUID;

public class ChkpMessage extends Message
{

  private static final long serialVersionUID = -6339978017034829113L;
  private UUID uuid = UUID.randomUUID();
  
  private int chkpId;
  
  public ChkpMessage(int id)
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
