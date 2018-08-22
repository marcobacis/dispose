
package dispose.net.node.datasources;

import dispose.net.common.DataAtom;
import dispose.net.node.ComputeNode;


public interface DataSource extends ComputeNode
{
  public DataAtom nextAtom();


  /** Performs initialization procedures (e.g. opening files/urls etc..) */
  public void setUp();


  /** Performs actions at the end of the computation */
  public void end();

  /** Restarts the source (cleans the state, e.g. files corrupted) */
  public void restart();

  /** @return The data type of the output */
  public Class<? extends DataAtom> outputRestriction();
}
