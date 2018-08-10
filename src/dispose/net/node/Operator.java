package dispose.net.node;

import java.io.Serializable;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.TypeSet;

/**
 * State machine implementing an operator.
 */
public interface Operator extends Serializable
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
  
  /**
   * Processes one data atom, returns the processed output, and advances the
   * clock.
   * @param input The data atom/s to be processed.
   * @return The result of the processing.
   */
  public List<DataAtom> processAtom(DataAtom... input);
  
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
