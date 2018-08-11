package dispose.net.node;

import java.io.Serializable;

public interface ComputeNode extends Serializable
{
  public int getID();
  
  /** @return The clock of this object. */
  public int clock();
}
