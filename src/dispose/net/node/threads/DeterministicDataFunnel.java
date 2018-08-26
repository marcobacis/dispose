package dispose.net.node.threads;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.common.types.FloatData;
import dispose.net.common.types.NullData;
import dispose.net.node.checkpoint.OperatorCheckpoint;


/** A class which deterministically funnels N DataAtoms at once from N producers 
 * to a single consumer. The DataAtoms are ordered FIFO with respect to data
 * atoms received from the same producer, and in producer index number order
 * with respect to data atoms received from different producers. */
public class DeterministicDataFunnel implements Serializable
{
  private static final long serialVersionUID = 5354298488928186074L;
  private List<ConcurrentLinkedQueue<DataAtom>> inputQueues;
  private DataAtom[] inputAtoms;
  private Boolean[] readyAtoms;
  private Boolean[] endAtoms;
  private boolean killFlag = false;
  
  
  public DeterministicDataFunnel(int numInputs)
  {
    inputQueues = new ArrayList<>(numInputs);
    for (int d = 0; d < numInputs; d++) {
      inputQueues.add(new ConcurrentLinkedQueue<>());
    }
    
    inputAtoms = new DataAtom[numInputs];
    readyAtoms = new Boolean[numInputs];
    endAtoms = new Boolean[numInputs];

    for (int d = 0; d < numInputs; d++) {
      inputAtoms[d] = new NullData(-1);
      readyAtoms[d] = false;
      endAtoms[d] = false;
    }
  }
  
  
  public void addInFlightFromCheckpoint(OperatorCheckpoint checkpoint)
  {
    List<ConcurrentLinkedQueue<DataAtom>> inFlight = checkpoint.getInFlight();
    
    for(int q = 0; q < inputQueues.size(); q++) {
      inputQueues.get(q).addAll(inFlight.get(q));
    }
  }
  
  
  /** Puts a data atom from a specific producer into the funnel.
   * @param idx The index number of the producer
   * @param element The data atom to funnel */
  public synchronized void receivedAtom(int idx, DataAtom element)
  {
    this.inputQueues.get(idx).offer(element);
    this.notify();
  }
  
  
  public DataAtom[] getAtoms()
  {
    for (int i = 0; i < inputAtoms.length; i++) {
      if (!readyAtoms[i]) {
        ConcurrentLinkedQueue<DataAtom> queue = inputQueues.get(i);
        DataAtom inAtom = queue.peek();
        if (inAtom != null && !(inAtom instanceof NullData)) {
          if (inAtom instanceof FloatData) {
            inputAtoms[i] = inAtom;
          } else if (inAtom instanceof EndData) {
            inputAtoms[i] = new NullData(-1);
            endAtoms[i] = true;
          }
          readyAtoms[i] = true;
          queue.remove();
        }
      }
    }
    
    return inputAtoms;
  }
  
  
  public DataAtom[] getAtomsBlocking()
  {
    DataAtom[] ret = getAtoms();
    
    if(!unblockCondition()){
      synchronized(this) {
        while(!unblockCondition()) {
          try{
            this.wait();
          } catch (InterruptedException e) {
            return ret;
          }
          ret = getAtoms();
        }
      }
    }
    
    return ret;
  }
  
  
  private boolean unblockCondition()
  {
    return stopCondition() || processCondition() || killFlag;
  }
  
  
  /** @returns true if an EndData atom has been received from all producers */
  public boolean stopCondition()
  {
    boolean stopCondition = true;
    for(Boolean end : endAtoms)
      stopCondition &= end;
    
    return stopCondition;
  }
  
  
  /** @returns true if there is a data atom available from all the producers */
  public boolean processCondition()
  {
    boolean condition = true;
    for(Boolean ready : readyAtoms)
      condition &= ready;
    
    return condition;
  }
  
  
  public void resetAfterProcessing()
  {
    for (int j = 0; j < inputAtoms.length; j++) {
      if (!endAtoms[j]) {
        readyAtoms[j] = false;
        inputAtoms[j] = new NullData(-1);
      }
    }
  }
  
  
  /** Interrupts getAtomsBlocking() even if not all DataAtoms have been
   * properly received. After this method has been called, the funnel cannot
   * be used anymore. */
  synchronized public void forceStop()
  {
    killFlag = true;
    notifyAll();
  }
  
}
