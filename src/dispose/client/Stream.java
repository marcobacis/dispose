package dispose.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a Stream, and also a node in the DAG representation of the computation.
 * We assume all the operations to be window-based (operations on a single item at
 * a time have a window of size 1).
 */

public class Stream
{
  
  private List<Stream> parents = new ArrayList<Stream>();
  private List<Stream> children = new ArrayList<Stream>();
  
  private Op operation;
  
  private int id;
  
  private int windowSize;
  
  /**
   * Enumerator class used to assign a unique id (integer)
   * to every new stream.
   */
  private static class StreamEnumerator {
    private static int currID = 0;

    public static int getNewID() {
      int current = currID;
      currID++;
      return current;
    }
  }
  
  /**
   * Dummy constructor
   */
  public Stream() {
    this.id = StreamEnumerator.getNewID();
    this.operation = Op.NONE;
    this.windowSize = 1;
  }
  
  /**
   * @param op      Operator to use on the data passing trough the node
   * @param winSize Size of the window for the operation
   * @param parents Previous streams from which this one is derived
   */
  public Stream(Op op, int winSize, Stream ...parents) {
    this.id = StreamEnumerator.getNewID();
    this.parents = Arrays.asList(parents);
    this.operation = op;
    this.windowSize = winSize;
    
    for(Stream p : parents) {
      p.addChild(this);
    }
    
  }

  /**
   * Applies an operation on the stream, creating a new stream
   * @param op          Operator applied to the stream
   * @param windowSize  Size of the operator window
   * @return            The resulting stream
   */
  public Stream apply(Op op, int windowSize) {
    Stream child = new Stream(op, windowSize, this);
    this.children.add(child);
    return child;
  }
  
  /**
   * Joins the stream with one or more streams based on the key,
   * returning the joined stream.
   * 
   * @param streams     The streams to be joined to the current one
   * @return            A new stream representing the joined streams
   */
  public Stream join(Stream... streams) {
    Stream[] parents = new Stream[streams.length+1];
    
    parents[0] = this;
    
    for(int i = 1; i < parents.length; i++) {
      parents[i] = streams[i-1];
    }
    
    return new Stream(Op.NONE, 1, parents);
  }
  
  public void addChild(Stream child) {
    this.children.add(child);
  }
  
  /**
   * Returns the parent streams
   */
  public List<Stream> getParents() {
    return this.parents;
  }
  
  /**
   * Returns the current operation's window size
   */
  public int getWindowSize() {
    return this.windowSize;
  }
  
  /**
   * Returns the operation applied on this stream
   */
  public Op getOperation() {
    return this.operation;
  }
  
  /**
   * Returns the (only) root of the streams dag
   */
  public Stream traceSource() {
    if(this.parents.isEmpty())
      return this;
    
    return this.parents.get(0).traceSource();
  }
  
  /**
   * Returns the stream's unique id
   */
  public int getID() {
    return this.id;
  }
  
}
