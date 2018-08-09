package dispose.net.message;

import java.io.Serializable;
import java.util.UUID;

public abstract class Message implements Serializable
{
  private static final long serialVersionUID = 2104456144558536350L;
  
  
  public abstract UUID getUUID();
}
