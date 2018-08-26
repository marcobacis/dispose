
package dispose.net.common.types;

import dispose.net.common.DataAtom;


public class NullData extends DataAtom
{
  private static final long serialVersionUID = 7909128997868538548L;


  public NullData(long timestamp)
  {
    super(timestamp);
  }


  public String toString()
  {
    return "NullData";
  }
}
