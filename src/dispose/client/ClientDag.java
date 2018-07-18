package dispose.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class ClientDag
{
  
  /**
   * Returns an instance of ClientDag derived from the given stream backwards
   * @param sink    Consumer stream from which to get all the parent nodes for the dag
   */
  public static ClientDag derive(Stream sink) {
    Set<Integer> IDs = new HashSet<>();
    
    //Depth-first visit to add nodes to the dag
    List<Stream> nodes = new ArrayList<>();
    
    Stack<Stream> visitStack = new Stack<>();
    
    visitStack.push(sink);
    
    while(!visitStack.isEmpty()) {
      Stream curr = visitStack.pop();
      
      nodes.add(curr);
      
      for(Stream p : curr.getParents()) {
        int id = p.getID();
        if(!IDs.contains(id)) {
          IDs.add(id);
          visitStack.push(p);
        }
      }
      
    }
    
    return new ClientDag(nodes,sink);
  }
  
  private List<Stream> nodes;
  
  private Stream source;
  private Stream sink;
  
  /**
   * Private constructor, the dag can be created only by deriving it
   * @param nodes   List of nodes to set
   * @param sink    Sink node from which the dag is derived
   */
  private ClientDag(List<Stream> nodes, Stream sink) {
    this.nodes = nodes;
    this.source = sink.traceSource();
    this.sink = sink;
  }
  
  /**
   * Serializes the dag in the format
   * sourceID, sinkID, (node), (node), ...
   */
  public String serialize() {
    String nodesSerial = nodes.stream().map(node -> node.serialize()).collect(Collectors.joining(","));
    String sourceID = Integer.toString(source.getID());
    String sinkID = Integer.toString(sink.getID());
    
    return String.join(",", sourceID, sinkID, nodesSerial);
  }
  
  
}
