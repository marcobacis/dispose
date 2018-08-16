package dispose.net.node;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

import dispose.net.node.operators.Operator;

public class OperatorCheckpoint implements Serializable
{

  private static final long serialVersionUID = -3316112612186425718L;

  private long timestamp = last_timestamp++;
  private static long last_timestamp = 0;

  private int id;
  private Operator op;
  private List<AtomsCache> outCaches;
  
  public OperatorCheckpoint(int id, Operator operator, List<AtomsCache> caches)
  {
    this.id = id;
    this.op = operator;
    this.outCaches = caches;
  }
  
  /**
   * Reads and return a checkpoint from the given path
   * @param path    Location of the checkpoint to load
   * @return        The read checkpoint
   * @throws IOException
   */
  public static OperatorCheckpoint loadFrom(String path) throws IOException
  {
    FileInputStream inFile = new FileInputStream(path);
    
    ObjectInputStream chkpStream = new ObjectInputStream(inFile);
    
    OperatorCheckpoint chkp = null;
    try {
      
      chkp = (OperatorCheckpoint) chkpStream.readObject();
    
    } catch (ClassNotFoundException e) {
      //difficult to be reachable....
      e.printStackTrace();
    }
    
    chkpStream.close();
    inFile.close();
    
    return chkp;
  }
  
  /**
   * Saves the checkpoint to the given path
   * @param path    The path to which the checkpoint is saved
   * @throws IOException
   */
  public void saveTo(String path) throws IOException {
    
    FileOutputStream outFile = new FileOutputStream(path);
    
    ObjectOutputStream chkpStream = new ObjectOutputStream(outFile);
    
    chkpStream.writeObject(this);
    
    chkpStream.close();
    outFile.close();
  }
  
  public int getID()
  {
    return this.id;
  }
  
  public long getTimestamp()
  {
    return this.timestamp;
  }
  
  public List<AtomsCache> getCaches()
  {
    return this.outCaches;
  }
  
  public Operator getOperator()
  {
    return this.op;
  }
  
}
