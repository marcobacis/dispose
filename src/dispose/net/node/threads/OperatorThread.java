
package dispose.net.node.threads;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.net.common.*;
import dispose.net.common.types.*;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.links.MonitoredLink.Delegate;
import dispose.net.message.Message;
import dispose.net.node.ComputeThread;
import dispose.net.node.operators.Operator;

/**
 * Class representing a single operator thread.
 * A thread is instantiated for each operator on the node,
 * and the threads are linked by using Links.
 *
 */
public class OperatorThread extends ComputeThread
{
  private Operator operator;

  private List<Link> inLinks = new ArrayList<>();
  private List<MonitoredLink> inStreams = new ArrayList<>();
  private List<Link> outStreams = new ArrayList<>();

  private DataAtom[] inputAtoms;
  
  private AtomicBoolean running = new AtomicBoolean(true);

  
  public OperatorThread(Operator operator)
  {
    this.operator = operator;
    this.opID = operator.getID();
  }


  /**
   * Adds an input link to get the atoms from
   * @param inputLink   Input link to use
   * @throws IOException
   */
  public void addInput(Link inputLink) throws IOException
  {
    this.inLinks.add(inputLink);
  }


  /**
   * Adds an output link (there can be many) to write the results to
   * @param outputLink  The output link to use
   * @throws IOException
   */
  public void addOutput(Link outputLink) throws IOException
  {
    this.outStreams.add(outputLink);
  }

  /**
   * Sets the current cycle of the thread as the last one
   */
  public void kill()
  {
    this.running.set(false);
  }
  
  
  private class OperatorDelegate implements Delegate
  {
    OperatorThread op;
    int StreamIndex;
    
    
    OperatorDelegate(OperatorThread op, int idx)
    {
      this.op = op;
      this.StreamIndex = idx;
    }
    
    
    @Override
    public void messageReceived(Message msg) throws Exception
    {
      
      if(msg instanceof DataAtom) {
        System.out.println("Message received -> " + ((FloatData) msg).floatValue());
        this.op.notifyElement(StreamIndex, ((DataAtom) msg));
        this.op.process();
      }
    }


    @Override
    public void linkIsBroken(Exception e)
    {
      System.out.println("Fuck the " + this.StreamIndex + "th link on operator " + op.getID() + " is broken");
    }
  }
  
  
  public void start() {
    System.out.println("Start called on thread " + getID());
    inputAtoms = new DataAtom[this.inLinks.size()];
    
    for(int i = 0; i < this.inLinks.size(); i++) {
      this.inStreams.add(MonitoredLink.asyncMonitorLink(this.inLinks.get(i), new OperatorDelegate(this, i)));
    }
  }
  
  
  private void notifyElement(int idx, DataAtom element) {
    this.inputAtoms[idx] = element;
  }
  
  
  /**
   * Main loop of the thread. Gets the control commands
   * and runs the operator on each new data on the input link.
   */
  private void process() {
    if (true) {
      System.out.println("Processing in operator " + getID());
      // I/O processing
      try {
        
        List<DataAtom> result = this.operator.processAtom(inputAtoms);
        
        if(result.size() > 0) {
          for(DataAtom resAtom : result) {
            if (resAtom != null) {
              for(Link out : this.outStreams) {
                System.out.println("Sending out -> " + resAtom);
                out.sendMsg(resAtom);
              }
            }
          }
        }
        
      } catch(Exception e) {
        System.out.println("Exception while processing " + e.getMessage());
        e.printStackTrace();
      }
    }
  }

}
