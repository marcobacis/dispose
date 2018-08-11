package dispose.net.node.datasources;

import dispose.net.common.DataAtom;
import dispose.net.node.ComputeNode;

public interface DataSource extends ComputeNode
{
  public DataAtom nextAtom();
  
  /**
   * @return The data type of the output
   */
  public Class<? extends DataAtom> outputRestriction();
}
