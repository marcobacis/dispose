
package dispose.net.common.types;

import dispose.net.common.DataAtom;


public class EndData extends DataAtom
{
  private static final long serialVersionUID = 751854214266997777L;


  public EndData(long timestamp)
  {
    super(timestamp);
  }


  public String toString()
  {
    return "End Data";
  }
}
