package dispose.net.node;

import java.io.Serializable;

import dispose.net.common.DataAtom;

public interface DataSink extends Serializable
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
  
  public void processAtom(DataAtom atom);
  
  /**
   * @return True if the specified input class is allowed
   */
  public boolean inputRestriction(Class<? extends DataAtom> input);
}
