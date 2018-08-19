package dispose.net.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import dispose.client.ClientDag;
import dispose.net.links.MonitoredLink.AckType;
import dispose.net.message.ConnectRemoteThreadsMsg;
import dispose.net.message.ConnectThreadsMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.DeployDataSinkThreadMsg;
import dispose.net.message.DeployDataSourceThreadMsg;
import dispose.net.message.DeployOperatorThreadMsg;
import dispose.net.message.StartThreadMsg;
import dispose.net.node.ComputeNode;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.operators.Operator;
import dispose.net.supervisor.JobDag.LinkDescription;

public class Job
{
  private UUID id;
  private Supervisor supervis;
  private NodeProxy owner;
  private JobDag jobDag;
  private Map<Integer, NodeProxy> logNodeToPhysNode;
  
  
  /* TODO: garbage collection on failure */
  /* TODO: retry on failure with retry count */
  
  
  public Job(UUID id, JobDag jobDag, Map<Integer, NodeProxy> initialNodeAlloc, Supervisor supervis, NodeProxy owner)
  {
    this.id = id;
    this.jobDag = jobDag;
    this.logNodeToPhysNode = initialNodeAlloc;
    this.supervis = supervis;
    this.owner = owner;
  }
  
  
  public static Job jobFromClientDag(UUID id, ClientDag dag, Supervisor supervis, NodeProxy owner) throws InvalidDagException
  {
    JobDag jobDag = new JobDag(dag);
    Job job = new Job(id, jobDag, new HashMap<>(), supervis, owner);
    return job;
  }
  
  
  public UUID getID()
  {
    return id;
  }
  
  
  private void reallocateAllNodes() throws Exception
  {
    List<NodeProxy> physNodes = new ArrayList<>(supervis.getNodes());
    if (physNodes.size() < 1)
      throw new Exception("how can I instantiate a dag if there are no nodes to use?!");
    
    logNodeToPhysNode = new HashMap<>();
    
    // TODO: Use a topological ordering to exploit logical node locality
    // TODO: Use an abstract computation power per node metric to perform static load balancing
    double lnodePerPnode = Double.max(1.0, (double)(jobDag.getNodes().size() - 2) / (double)physNodes.size());
    double lnodesLeft = lnodePerPnode;
    int currPnode = 0;
    
    for (ComputeNode lnode: jobDag.getNodes()) {
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
    }
  }
  
  
  public void materialize() throws Exception
  {
    reallocateAllNodes();
    materializeAllNodes();
    materializeAllLinks();
  }
  
  
  private void materializeAllNodes() throws Exception
  {
    Collection<ComputeNode> logNodes = jobDag.getNodes();
    
    for (ComputeNode logNode: logNodes) {
      NodeProxy physNode = logNodeToPhysNode.get(logNode.getID());
      CtrlMessage msg;
      if (logNode instanceof DataSink) {
        msg = new DeployDataSinkThreadMsg((DataSink) logNode);
      } else if (logNode instanceof DataSource) {
        msg = new DeployDataSourceThreadMsg((DataSource) logNode);
      } else {
        msg = new DeployOperatorThreadMsg((Operator) logNode);
      }
      physNode.getLink().sendMsgAndRequestAck(msg);
      // TODO: wait acks after sending all the messages
      physNode.getLink().waitAck(msg);
    }
  }
  
  
  private void materializeAllLinks() throws Exception
  {
    Collection<LinkDescription> links = jobDag.getLinks();
    List<LinkDescription> localLinks = new ArrayList<>();
    List<LinkDescription> remoteLinks = new ArrayList<>();
    
    for (LinkDescription link: links) {
      NodeProxy ln1pn = logNodeToPhysNode.get(link.getSourceNodeId());
      NodeProxy ln2pn = logNodeToPhysNode.get(link.getDestinationNodeID());
      if (ln1pn == ln2pn)
        localLinks.add(link);
      else
        remoteLinks.add(link);
    }
    
    materializeLocalLinks(localLinks);
    materializeRemoteLinks(remoteLinks);
  }
  
  
  private void materializeLocalLinks(Collection<LinkDescription> localLinks) throws Exception
  {
    for (LinkDescription localLink: localLinks) {
      NodeProxy physNode = logNodeToPhysNode.get(localLink.getSourceNodeId());
      CtrlMessage msg = new ConnectThreadsMsg(localLink.getSourceNodeId(), localLink.getDestinationNodeID());
      physNode.getLink().sendMsg(msg);
    }
  }
  
  
  private void materializeRemoteLinks(Collection<LinkDescription> remoteLinks) throws Exception
  {
    /* TODO: parallelize link instantiation */
    
    int port = 9000;
    for (LinkDescription remoteLink: remoteLinks) {
      int snid = remoteLink.getSourceNodeId();
      int dnid = remoteLink.getDestinationNodeID();
      
      NodeProxy physNode1 = logNodeToPhysNode.get(snid);
      NodeProxy physNode2 = logNodeToPhysNode.get(dnid);

      CtrlMessage msg = new ConnectRemoteThreadsMsg(snid, dnid, physNode1.getNetworkAddress(), port);
      physNode2.getLink().sendMsgAndRequestAck(msg, AckType.RECEPTION);
      physNode2.getLink().waitAck(msg);
      
      CtrlMessage msg2 = new ConnectRemoteThreadsMsg(snid, dnid, physNode2.getNetworkAddress(), port);
      physNode1.getLink().sendMsgAndRequestAck(msg2, AckType.PROCESSING);
      physNode1.getLink().waitAck(msg2);
      
      port++;
    }
  }
  
  
  public void start() throws Exception
  {
    Set<NodeProxy> pnodes = new HashSet<>(logNodeToPhysNode.values());
    for (NodeProxy pnode: pnodes) {
      ArrayList<Integer> lnodes = new ArrayList<>();
      for (Entry<Integer, NodeProxy> lnpn: logNodeToPhysNode.entrySet()) {
        if (pnode == lnpn.getValue())
          lnodes.add(lnpn.getKey());
      }
      
      CtrlMessage cmsg = new StartThreadMsg(lnodes);
      pnode.getLink().sendMsg(cmsg);
    }
  }
}
