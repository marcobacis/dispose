package dispose.net.node;

import java.util.UUID;

import dispose.net.links.Link;
import dispose.net.node.checkpoint.Checkpoint;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.ClosedEndException;
import dispose.net.node.threads.OperatorThread;
import dispose.net.node.threads.SinkThread;
import dispose.net.node.threads.SourceThread;

public abstract class ComputeThread
{
  protected int opID;
  protected Node owner;
  protected UUID jid;
  
  public ComputeThread(Node owner, UUID jid)
  {
    this.owner = owner;
    this.jid = jid;
  }
  
  public static ComputeThread createComputeThread(Node owner, UUID jid, ComputeNode compnode) {
    if(compnode instanceof Operator)
      return new OperatorThread(owner, jid, (Operator) compnode);
    else if (compnode instanceof DataSource)
      return new SourceThread(owner, jid, (DataSource) compnode);
    else
      return new SinkThread(owner, jid, (DataSink) compnode);
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
  public final int getID()
  {
    return this.opID;
  }
  
  public final UUID getJobID()
  {
    return jid;
  }
  
  /**
   * Reloads the compute thread state by using the given checkpoint
   * @param chkp    The checkpoint to use to restore the state
   */
  public abstract void reloadFromCheckpoint(Checkpoint chkp);
}
