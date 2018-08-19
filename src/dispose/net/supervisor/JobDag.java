package dispose.net.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dispose.client.ClientDag;
import dispose.client.Op;
import dispose.client.Stream;
import dispose.net.node.ComputeNode;
import dispose.net.node.datasinks.SinkFactory;
import dispose.net.node.datasources.SourceFactory;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.JoinOperator;
import dispose.net.node.operators.MaxWindowOperator;
import dispose.net.node.operators.MinWindowOperator;
import dispose.net.node.operators.NullOperator;
import dispose.net.node.operators.SumWindowOperator;

public class JobDag
{
  private List<LinkDescription> links = new ArrayList<>();
  private Map<Integer, ComputeNode> logNodes = new HashMap<>();
  
  
  public class LinkDescription
  {
    private int source;
    private int dest;
    
    LinkDescription(int source, int dest)
    {
      this.source = source;
      this.dest = dest;
    }
    
    
    public int getSourceNodeId()
    {
      return source;
    }
    
    
    public int getDestinationNodeID()
    {
      return dest;
    }
    
    
    public String toString()
    {
      return Integer.toString(source) + "->" + Integer.toString(dest);
    }
  }
  
  
  public JobDag(ClientDag dag) throws InvalidDagException
  {
    List<Stream> streams = dag.getNodes();
    for (Stream stream: streams) {
      ComputeNode logNode;
      
      if (dag.isSource(stream)) {
        logNode = SourceFactory.getFromStream(stream);
      } else if (dag.isSink(stream)) {
        logNode = SinkFactory.getFromStream(stream);
      } else {
        Op operation = stream.getOperation();
        switch (operation) {
          case AVG:
            logNode = new AvgWindowOperator(stream.getID(), stream.getWindowSize(), stream.getWindowSlide());
            break;
          case MAX:
            logNode = new MaxWindowOperator(stream.getID(), stream.getWindowSize(), stream.getWindowSlide());
            break;
          case MIN:
            logNode = new MinWindowOperator(stream.getID(), stream.getWindowSize(), stream.getWindowSlide());
            break;
          case NONE:
            if(stream.getParents().size() > 1) {
              logNode = new JoinOperator(stream.getID(), stream.getWindowSize(), stream.getWindowSlide(), stream.getParents().size());
            } else {
              logNode = new NullOperator(stream.getID());
            }
            break;
          case SUM:
            logNode = new SumWindowOperator(stream.getID(), stream.getWindowSize(), stream.getWindowSlide());
            break;
          default:
            throw new InvalidDagException("unknown op");
        }
      }
      
      logNodes.put(logNode.getID(), logNode);
      List<Stream> children = stream.getChildren();
      for (Stream child: children) {
        LinkDescription ld = new LinkDescription(stream.getID(), child.getID());
        links.add(ld);
      }
    }
  }
  
  
  public Collection<ComputeNode> getNodes()
  {
    return Collections.unmodifiableCollection(logNodes.values());
  }
  
  
  public Collection<LinkDescription> getLinks()
  {
    return Collections.unmodifiableCollection(links);
  }
  
  
  public ComputeNode getNodeFromId(int id)
  {
    return logNodes.get(id);
  }
}
