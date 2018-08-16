package dispose.net.node.operators;

import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.TypeSet;
import dispose.net.node.ComputeNode;

/**
 * State machine implementing an operator.
 */
public interface Operator extends ComputeNode
{
  /**
   * Processes one data atom, returns the processed output, and advances the
   * clock.
   * @param input The data atom/s to be processed.
   * @return The result of the processing.
   */
  public List<DataAtom> processAtom(DataAtom... input);
  
  /**
   * @return the number of inputs that are given to the operator
   */
  public int getNumInputs();
  
  /**
   * @return The set of possible data atom classes that this operator can
   * process. 
   */
  public TypeSet inputRestriction();
  
  /**
   * @return The data type of the output when the input is of the specified
   * data type.
   */
  public Class<? extends DataAtom> outputRestriction(Class<? extends DataAtom> intype);
}
