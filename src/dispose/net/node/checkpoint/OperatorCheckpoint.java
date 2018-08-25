
package dispose.net.node.checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import dispose.net.common.DataAtom;
import dispose.net.common.DeepCopy;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.DeterministicDataFunnel;


public class OperatorCheckpoint extends Checkpoint
{
  private static final long serialVersionUID = -3316112612186425718L;

  private List<ConcurrentLinkedQueue<DataAtom>> inFlight;
  private boolean[] checked;
  private DeterministicDataFunnel inputState;

  public OperatorCheckpoint(UUID id, Operator operator,
    DeterministicDataFunnel inputState)
  {
    super(id, operator);
    this.inputState = (DeterministicDataFunnel) DeepCopy.copy(inputState);
    this.inFlight = new ArrayList<>(operator.getNumInputs());
    this.checked = new boolean[operator.getNumInputs()];
    for (int i = 0; i < operator.getNumInputs(); i++) {
      this.inFlight.add(new ConcurrentLinkedQueue<>());
      this.checked[i] = false;
    }
  }


  public synchronized void addInFlightAtom(int idx, DataAtom atom)
  {
    if(!checked[idx])
      inFlight.get(idx).offer(atom);
  }


  public void notifyCheck(int idx)
  {
    this.checked[idx] = true;
  }


  public boolean isComplete()
  {
    boolean completed = true;
    for (boolean check : checked)
      completed &= check;

    return completed;
  }


  public synchronized List<ConcurrentLinkedQueue<DataAtom>> getInFlight()
  {
    return inFlight;
  }
  
  
  public DeterministicDataFunnel getInputState()
  {
    return inputState;
  }

}
