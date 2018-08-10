package dispose.net.node;

import java.io.Serializable;

import dispose.net.common.DataAtom;

public interface DataSource extends Serializable
{
  /**
   * Returns the operator id as in the job DAG
   * @return the operator's id
   */
  public int getID();
  
  /**
   * @return The clock of this object.
   */
  public int clock();
  
  public DataAtom nextAtom();
  
  /**
   * @return The data type of the output
   */
  public Class<? extends DataAtom> outputRestriction();
}
