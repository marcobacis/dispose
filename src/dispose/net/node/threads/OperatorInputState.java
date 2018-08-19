package dispose.net.node.threads;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.common.types.FloatData;
import dispose.net.common.types.NullData;

public class OperatorInputState implements Serializable
{

  private static final long serialVersionUID = 5354298488928186074L;

  private int numInputs;
  
  private List<ConcurrentLinkedQueue<DataAtom>> inputQueues;
  
  private DataAtom[] inputAtoms;
  
  private Boolean[] readyAtoms;
  
  private Boolean[] endAtoms;
  
  public OperatorInputState(List<ConcurrentLinkedQueue<DataAtom>> inQueues)
  {
    this.inputQueues = inQueues;
    this.numInputs = inQueues.size();
    
    inputAtoms = new DataAtom[numInputs];
    readyAtoms = new Boolean[numInputs];
    endAtoms = new Boolean[numInputs];

    for (int d = 0; d < numInputs; d++) {
      inputAtoms[d] = new NullData();
      readyAtoms[d] = false;
      endAtoms[d] = false;
    }
  }
  
  public DataAtom[] recvAtoms()
  {
    for (int i = 0; i < inputAtoms.length; i++) {
      if (!readyAtoms[i]) {
        ConcurrentLinkedQueue<DataAtom> queue = inputQueues.get(i);
        
        //synchronized(queue) {
          DataAtom inAtom = queue.peek();
          if (inAtom != null && !(inAtom instanceof NullData)) {
            if (inAtom instanceof FloatData) {
              inputAtoms[i] = inAtom;
            } else if (inAtom instanceof EndData) {
              inputAtoms[i] = new NullData();
              endAtoms[i] = true;
            }
            readyAtoms[i] = true;
            queue.remove();
          }
        //}
      }
    }
    
    return inputAtoms;
  }
  
  public DataAtom[] recvAtomsBlocking()
  {
    DataAtom[] ret = recvAtoms();
    
    if(!unblockCondition()){
      synchronized(this) {
        while(!unblockCondition()) {
          try{
            this.wait();
          } catch (InterruptedException e) {
            return ret;
          }
          ret = recvAtoms();
        }
      }
    }
    
    return recvAtoms();
    
  }
  
  private boolean unblockCondition()
  {
    return stopCondition() || processCondition();
  }
  
  public boolean stopCondition()
  {
    boolean stopCondition = true;
    for(Boolean end : endAtoms)
      stopCondition &= end;
    
    return stopCondition;
  }
  
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
        inputAtoms[j] = new NullData();
      }
    }
  }
  
}
