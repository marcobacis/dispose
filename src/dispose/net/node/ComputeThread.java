package dispose.net.node;

import dispose.net.links.Link;
import dispose.net.node.threads.ClosedEndException;

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
   * @param fromId TODO
   * @throws ClosedEndException if the thread cannot accept inputs
   */
  public abstract void addInputLink(Link inputLink, int fromId) throws ClosedEndException;
  
  
  /**
   * Adds an output link (there can be many) to write the results to
   * @param outputLink  The output link to use
   * @param toId TODO
   * @throws ClosedEndException if the thread cannot accept outputs
   */
  public abstract void addOutputLink(Link outputLink, int toId) throws ClosedEndException;
  
  
  /**
   * Pauses the computation thread
   */
  public abstract void pause();
  
  /**
   * Resumes the computation thread
   */
  public abstract void resume();
  
  /**
   * Starts the computation thread
   */
  public abstract void start();
  
  /**
   * Sets this cycle as the last one, then stops the computation
   */
  public abstract void stop();
    
  /**
   * Returns the operator's id in the job's dag
   * @return the operator id
   */
  public final int getID() {
    return this.opID;
  }
}
