package dispose.net.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import dispose.net.node.ComputeNode;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.supervisor.JobDag.LinkDescription;

public class JobDagAllocation
{
  private JobDag jobDag;
  private Map<Integer, NodeProxy> logNodeToPhysNode = new HashMap<>();
  private Set<Integer> liveLogNodes = new HashSet<>();
  private Set<LinkDescription> liveLinks = new HashSet<>();
  
  
  public JobDagAllocation(JobDag jobDag)
  {
    this.jobDag = jobDag;
  }
  
  
  public JobDag getJobDag()
  {
    return jobDag;
  }
  
  
  public Set<Integer> getLiveLogicalNodes()
  {
    return Collections.unmodifiableSet(new HashSet<>(liveLogNodes));
  }
  
  
  public Set<LinkDescription> getLiveLinks()
  {
    return Collections.unmodifiableSet(new HashSet<>(liveLinks));
  }
  
  
  public Set<NodeProxy> livePhysicalNodes()
  {
    return Collections.unmodifiableSet(new HashSet<>(logNodeToPhysNode.values()));
  }
  
  
  public NodeProxy getPhysicalNodeHostingLogicalNodeId(int nid)
  {
    return logNodeToPhysNode.get(nid);
  }
  
  
  public Set<Integer> logicalNodesHostedInPhysicalNode(NodeProxy pnode)
  {
    HashSet<Integer> lnodes = new HashSet<>();
    for (Entry<Integer, NodeProxy> lnpn: logNodeToPhysNode.entrySet()) {
      if (pnode == lnpn.getValue())
        lnodes.add(lnpn.getKey());
    }
    return lnodes;
  }
  
  
  public Set<LinkDescription> localLinks()
  {
    Collection<LinkDescription> links = jobDag.getLinks();
    Set<LinkDescription> localLinks = new HashSet<>();
    
    for (LinkDescription link: links) {
      NodeProxy ln1pn = logNodeToPhysNode.get(link.getSourceNodeId());
      NodeProxy ln2pn = logNodeToPhysNode.get(link.getDestinationNodeID());
      if (ln1pn == ln2pn)
        localLinks.add(link);
    }
    
    return localLinks;
  }
  
  
  public Set<LinkDescription> remoteLinks()
  {
    Collection<LinkDescription> links = jobDag.getLinks();
    Set<LinkDescription> remoteLinks = new HashSet<>();
    
    for (LinkDescription link: links) {
      NodeProxy ln1pn = logNodeToPhysNode.get(link.getSourceNodeId());
      NodeProxy ln2pn = logNodeToPhysNode.get(link.getDestinationNodeID());
      if (ln1pn != ln2pn)
        remoteLinks.add(link);
    }
    
    return remoteLinks;
  }
  
  
  public void allocateAllNodes(Collection<NodeProxy> physNodes, NodeProxy owner) throws ResourceUnderrunException
  {
    liveLogNodes.clear();
    allocateMissingNodes(physNodes, owner);
  }
  
  
  public void allocateMissingNodes(Collection<NodeProxy> physNodes0, NodeProxy owner) throws ResourceUnderrunException
  {
    if (physNodes0.size() < 1)
      throw new ResourceUnderrunException("how can I instantiate a dag if there are no nodes to use?!");
    
    List<NodeProxy> physNodes = new ArrayList<>(physNodes0);
    Collections.shuffle(physNodes);
    
    // TODO: Use a topological ordering to exploit logical node locality
    // TODO: Use an abstract computation power per node metric to perform static load balancing
    int deadNodesCnt = Integer.max(0, jobDag.getNodes().size() - liveLogNodes.size() - 2);
    double lnodePerPnode = Double.max(1.0, (double)(deadNodesCnt) / (double)physNodes.size());
    double lnodesLeft = lnodePerPnode;
    int currPnode = 0;
    
    for (ComputeNode lnode: jobDag.getNodes()) {
      if (liveLogNodes.contains(lnode.getID()))
        continue;
      
      if (lnode instanceof DataSink || lnode instanceof DataSource) {
        logNodeToPhysNode.put(lnode.getID(), owner);
      } else {
        NodeProxy myNode = physNodes.get(currPnode);
        logNodeToPhysNode.put(lnode.getID(), myNode);
        lnodesLeft -= 1.0;
        if (lnodesLeft < 0.5) {
          lnodesLeft += lnodePerPnode;
          currPnode = (currPnode + 1) % physNodes.size();
        }
      }
      
      liveLogNodes.add(lnode.getID());
    }
    
    updateLiveLinks();
  }
  
  
  private void updateLiveLinks()
  {
    Collection<LinkDescription> allLinks = jobDag.getLinks();
    liveLinks.clear();
    
    for (LinkDescription link: allLinks) {
      if (liveLogNodes.contains(link.getSourceNodeId()) &&
          liveLogNodes.contains(link.getDestinationNodeID()))
        liveLinks.add(link);
    }
  }
  
  
  public void removeDeadPhysicalNodes(Collection<NodeProxy> deadNodes)
  {
    Set<NodeProxy> deadNodesSet = new HashSet<>(deadNodes);
    Set<Integer> newLiveLogNodes = new HashSet<>();
    
    for (Integer logNode: liveLogNodes) {
      if (!deadNodesSet.contains(logNodeToPhysNode.get(logNode))) {
        newLiveLogNodes.add(logNode);
      }
    }
    
    liveLogNodes = newLiveLogNodes;
    updateLiveLinks();
  }
  
  @Override
  public String toString()
  {    
    List<Integer> physNodes = logNodeToPhysNode.values().stream().map((node) -> node.nodeID()).distinct().collect(Collectors.toList());
    
    List<String> nodes = new ArrayList<>(physNodes.size());
    
    for(Integer node : physNodes) {
      
      List<String> ops = new LinkedList<>();
      for(Integer op : liveLogNodes)
        if(logNodeToPhysNode.get(op).nodeID() == node)
          ops.add(Integer.toString(op));
      
        nodes.add("(" + Integer.toHexString(node) + ": " + String.join(",", ops) + ")");
    }
    
    return String.join(",", nodes);
  }
}
