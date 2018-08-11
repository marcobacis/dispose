package dispose.net.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dispose.client.ClientDag;
import dispose.client.Op;
import dispose.client.Stream;
import dispose.net.links.MonitoredLink.AckType;
import dispose.net.node.ComputeNode;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasinks.ObjectLogDataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.datasources.RandomFloatDataSrc;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.MaxWindowOperator;
import dispose.net.node.operators.MinWindowOperator;
import dispose.net.node.operators.NullOperator;
import dispose.net.node.operators.Operator;
import dispose.net.node.operators.SumWindowOperator;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class InstantiateDagMsg extends CtrlMessage
{
  private static final long serialVersionUID = 3594328279621054155L;
  private ClientDag dag;
  
  
  public InstantiateDagMsg(ClientDag dag)
  {
    this.dag = dag;
  }

  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws Exception
  {
    instantiateDag(supervis, nodem);
  }
  
  
  private void instantiateDag(Supervisor supervis, NodeProxy nodem) throws Exception
  {
    class LinkDescription
    {
      int node1;
      int node2;
      int port;
      
      LinkDescription(int n1, int n2)
      {
        node1 = n1;
        node2 = n2;
      }
      
      public String toString()
      {
        return Integer.toString(node1) + "->" + Integer.toString(node2);
      }
    }
 
    System.out.println("InstantiateDag!!");
    System.out.println(dag.toString());
    
    /* convert the DAG to a list of ComputeNodes and links */
    
    Map<Integer, Stream> streamIdToStream = new HashMap<>();
    List<LinkDescription> links = new ArrayList<>();
    List<ComputeNode> logNodes = new ArrayList<>();
    
    List<Stream> streams = dag.getNodes();
    for (Stream stream: streams) {
      streamIdToStream.put(stream.getID(), stream);
      ComputeNode logNode;
      
      if (dag.isSource(stream)) {
        logNode = new RandomFloatDataSrc(stream.getID(), 1000);
      } else if (dag.isSink(stream)) {
        logNode = new ObjectLogDataSink(stream.getID());
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
            logNode = new NullOperator(stream.getID());
            break;
          case SUM:
            logNode = new SumWindowOperator(stream.getID(), stream.getWindowSize(), stream.getWindowSlide());
            break;
          default:
            throw new Exception("unknown op");
        }
      }
      
      logNodes.add(logNode);
      List<Stream> children = stream.getChildren();
      for (Stream child: children) {
        LinkDescription ld = new LinkDescription(stream.getID(), child.getID());
        links.add(ld);
      }
    }
    
    /* Allocate each node to a physical node */
    
    List<NodeProxy> physNodes = new ArrayList<>(supervis.getNodes());
    if (physNodes.size() < 1)
      throw new Exception("how can I instantiate a dag if there are no nodes to use?!");
    
    Map<Integer, NodeProxy> logNodeToPhysNode = new HashMap<>();
    
    // TODO: Use a topological ordering to exploit logical node locality
    // TODO: Use an abstract computation power per node metric to perform static load balancing
    double lnodePerPnode = Double.max(1.0, (double)(logNodes.size() - 2) / (double)physNodes.size());
    double lnodesLeft = lnodePerPnode;
    int currPnode = 0;
    
    for (ComputeNode lnode: logNodes) {
      if (lnode instanceof DataSink || lnode instanceof DataSource) {
        logNodeToPhysNode.put(lnode.getID(), nodem);
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
    
    /* Partition the links in local links and remote links */
    
    List<LinkDescription> localLinks = new ArrayList<>();
    List<LinkDescription> remoteLinks = new ArrayList<>();
    
    for (LinkDescription link: links) {
      NodeProxy ln1pn = logNodeToPhysNode.get(link.node1);
      NodeProxy ln2pn = logNodeToPhysNode.get(link.node2);
      if (ln1pn == ln2pn)
        localLinks.add(link);
      else
        remoteLinks.add(link);
    }
    
    /* Materialize ComputeNodes */
    
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
    
    /* Materialize Local Links */
    
    for (LinkDescription localLink: localLinks) {
      NodeProxy physNode = logNodeToPhysNode.get(localLink.node1);
      CtrlMessage msg = new ConnectThreadsMsg(localLink.node1, localLink.node2);
      physNode.getLink().sendMsg(msg);
    }
    
    /* Materialize Global Links */
    
    int port = 9000;
    for (LinkDescription remoteLink: remoteLinks) {
      NodeProxy physNode1 = logNodeToPhysNode.get(remoteLink.node1);
      NodeProxy physNode2 = logNodeToPhysNode.get(remoteLink.node2);
      remoteLink.port = port;
      CtrlMessage msg = new ConnectRemoteThreadsMsg(remoteLink.node1, remoteLink.node2, physNode1.getNetworkAddress(), port);
      physNode2.getLink().sendMsgAndRequestAck(msg, AckType.RECEPTION);
      physNode2.getLink().waitAck(msg);
      port++;
    }
    
    for (LinkDescription remoteLink: remoteLinks) {
      NodeProxy physNode2 = logNodeToPhysNode.get(remoteLink.node2);
      CtrlMessage msg = new ConnectRemoteThreadsMsg(remoteLink.node1, remoteLink.node2, physNode2.getNetworkAddress(), remoteLink.port);
      physNode2.getLink().sendMsgAndRequestAck(msg, AckType.RECEPTION);
      physNode2.getLink().waitAck(msg);
    }
    
    /* And we're finally done! */
    /* TODO: garbage collection on failure */
    /* TODO: retry on failure with retry count */
  }
}
