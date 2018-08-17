package dispose.net.node;

import java.io.IOException;

import dispose.net.links.Link;

public abstract class ComputeThread
{
  protected int opID;
  protected Node owner;
  
  public ComputeThread(Node owner)
  {
    this.owner = owner;
  }
  
  /**
   * Adds an input link to get the atoms from
   * @param inputLink   Input link to use
   * @throws IOException
   */
  public abstract void addInput(Link inputLink) throws Exception;
  
  
  /**
   * Adds an output link (there can be many) to write the results to
   * @param outputLink  The output link to use
   * @throws IOException
   */
  public abstract void addOutput(Link outputLink) throws Exception;
  
  
  /**
   * Sets the current cycle of the thread as the last one
   */
  public abstract void pause();
  
  
  public abstract void start();
  
  
  /**
   * Returns the operator's id in the job's dag
   * @return the operator id
   */
  public final int getID() {
    return this.opID;
  }
}
