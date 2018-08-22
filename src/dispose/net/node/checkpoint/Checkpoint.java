
package dispose.net.node.checkpoint;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import dispose.net.common.DeepCopy;
import dispose.net.node.ComputeNode;


abstract public class Checkpoint implements Serializable
{

  private static final long serialVersionUID = 4952159123518118906L;
  protected long timestamp = last_timestamp++;
  protected static long last_timestamp = 0;

  private UUID id;
  private ComputeNode compnode;

  public Checkpoint(UUID chkpid, ComputeNode compnode)
  {
    this.id = chkpid;
    this.compnode = (ComputeNode) DeepCopy.copy(compnode);
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

  public ComputeNode getComputeNode()
  {
    return compnode;
  }
  
  public UUID getID()
  {
    return this.id;
  }


  public long getTimestamp()
  {
    return this.timestamp;
  }

}
