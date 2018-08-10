package dispose.net.node.datasinks;

import dispose.net.common.DataAtom;
import dispose.net.node.ComputeNode;

public interface DataSink extends ComputeNode
{
  public void processAtom(DataAtom atom);
  
  /**
   * @return True if the specified input class is allowed
   */
  public boolean inputRestriction(Class<? extends DataAtom> input);
}
