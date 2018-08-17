package dispose.net.node.datasources;

import dispose.net.common.DataAtom;
import dispose.net.node.ComputeNode;

public interface DataSource extends ComputeNode
{
  public DataAtom nextAtom();
  
  /**
   * Performs initialization procedures (e.g. opening files/urls etc..)
   */
  public void setUp();
  
  /**
   * Performs actions at the end of the computation
   */
  public void end();
  
  /**
   * @return The data type of the output
   */
  public Class<? extends DataAtom> outputRestriction();
}
