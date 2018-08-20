
package dispose.net.node;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import dispose.net.common.DataAtom;
import dispose.net.common.DeepCopy;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.OperatorInputState;


public class OperatorCheckpoint implements Serializable
{
  private static final long serialVersionUID = -3316112612186425718L;

  private long timestamp = last_timestamp++;
  private static long last_timestamp = 0;

  private UUID id;
  private Operator op;
  private List<ConcurrentLinkedQueue<DataAtom>> inFlight;
  private OperatorInputState inputState;
  private boolean[] checked;


  public OperatorCheckpoint(UUID id, Operator operator,
    OperatorInputState inputState)
  {
    this.id = id;
    this.op = (Operator) DeepCopy.copy(operator);
    this.inputState = (OperatorInputState) DeepCopy.copy(inputState);
    this.inFlight = new ArrayList<>(op.getNumInputs());
    this.checked = new boolean[op.getNumInputs()];
    for (int i = 0; i < op.getNumInputs(); i++) {
      this.inFlight.add(new ConcurrentLinkedQueue<>());
      this.checked[i] = false;
    }
  }


  /** Reads and return a checkpoint from the given path
   * @param path Location of the checkpoint to load
   * @return The read checkpoint
   * @throws IOException */
  public static OperatorCheckpoint loadFrom(String path) throws IOException
  {
    FileInputStream inFile = new FileInputStream(path);

    ObjectInputStream chkpStream = new ObjectInputStream(inFile);

    OperatorCheckpoint chkp = null;
    try {

      chkp = (OperatorCheckpoint) chkpStream.readObject();

    } catch (ClassNotFoundException e) {
      // difficult to be reachable....
      e.printStackTrace();
    }

    chkpStream.close();
    inFile.close();

    return chkp;
  }


  /** Saves the checkpoint to the given path
   * @param path The path to which the checkpoint is saved
   * @throws IOException */
  public void saveTo(String path) throws IOException
  {

    FileOutputStream outFile = new FileOutputStream(path);

    ObjectOutputStream chkpStream = new ObjectOutputStream(outFile);

    chkpStream.writeObject(this);

    chkpStream.close();
    outFile.close();
  }


  public UUID getID()
  {
    return this.id;
  }


  public long getTimestamp()
  {
    return this.timestamp;
  }


  public synchronized void addInFlightAtom(int idx, DataAtom atom)
  {
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


  public OperatorInputState getInputState()
  {
    return inputState;
  }


  public synchronized List<ConcurrentLinkedQueue<DataAtom>> getInFlight()
  {
    return inFlight;
  }


  public Operator getOperator()
  {
    return op;
  }

}
