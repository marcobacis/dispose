package dispose.net.node.datasinks;

import dispose.net.common.DataAtom;
import dispose.net.node.ComputeNode;

public interface DataSink extends ComputeNode
{
  public void processAtom(DataAtom atom, int sourceId);
  
  /**
   * Performs initialization procedures (e.g. opening files/urls etc..)
   */
  public void setUp();
  
  /**
   * Performs actions at the end of the computation
   */
  public void end();
  
  /**
   * @return True if the specified input class is allowed
   */
  public boolean inputRestriction(Class<? extends DataAtom> input);
}
