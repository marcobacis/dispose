package dispose.net.supervisor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import dispose.client.ClientDag;
import dispose.log.DisposeLog;
import dispose.log.LogInfo;
import dispose.net.links.LinkBrokenException;
import dispose.net.links.MonitoredLink.AckType;
import dispose.net.links.NotAcknowledgeableException;
import dispose.net.message.ConnectRemoteThreadsMsg;
import dispose.net.message.ConnectThreadsMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.DeployDataSinkThreadMsg;
import dispose.net.message.DeployDataSourceThreadMsg;
import dispose.net.message.DeployOperatorThreadMsg;
import dispose.net.message.ThreadCommandMsg;
import dispose.net.node.ComputeNode;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.operators.Operator;
import dispose.net.supervisor.JobDag.LinkDescription;

public class Job implements LogInfo
{
  private UUID id;
  private Supervisor supervis;
  private NodeProxy owner;
  private JobDag jobDag;
  private JobDagAllocation allocation;
  
  
  /* TODO: garbage collection on failure */
  /* TODO: retry on failure with retry count */
  
  
  public Job(UUID id, JobDag jobDag, JobDagAllocation initialNodeAlloc, Supervisor supervis, NodeProxy owner)
  {
    this.id = id;
    this.jobDag = jobDag;
    this.allocation = initialNodeAlloc;
    this.supervis = supervis;
    this.owner = owner;
  }
  
  
  @Override
  public String loggingName()
  {
    return "Job " + id.toString();
  }
  
  
  public static Job jobFromClientDag(UUID id, ClientDag dag, Supervisor supervis, NodeProxy owner) throws InvalidDagException
  {
    JobDag jobDag = new JobDag(dag);
    Job job = new Job(id, jobDag, new JobDagAllocation(jobDag), supervis, owner);
    return job;
  }
  
  
  public UUID getID()
  {
    return id;
  }
  
  
  public void materialize() throws LinkBrokenException, ResourceUnderrunException
  {
    allocation.allocateAllNodes(supervis.getNodes(), owner);
    materializeAllNodes();
    materializeLocalLinks(allocation.localLinks());
    materializeRemoteLinks(allocation.remoteLinks());
  }
  
  
  private void materializeAllNodes() throws LinkBrokenException
  {
    Collection<ComputeNode> logNodes = jobDag.getNodes();
    
    for (ComputeNode logNode: logNodes) {
      NodeProxy physNode = allocation.getPhysicalNodeHostingLogicalNodeId(logNode.getID());
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
      try {
        physNode.getLink().waitAck(msg);
      } catch (NotAcknowledgeableException e) {
        e.printStackTrace();
      }
    }
  }
  
  
  private void materializeLocalLinks(Collection<LinkDescription> localLinks) throws LinkBrokenException
  {
    for (LinkDescription localLink: localLinks) {
      NodeProxy physNode = allocation.getPhysicalNodeHostingLogicalNodeId(localLink.getSourceNodeId());
      CtrlMessage msg = new ConnectThreadsMsg(localLink.getSourceNodeId(), localLink.getDestinationNodeID());
      physNode.getLink().sendMsg(msg);
    }
  }
  
  
  private void materializeRemoteLinks(Collection<LinkDescription> remoteLinks) throws LinkBrokenException
  {
    /* TODO: parallelize link instantiation */
    
    int port = 9000;
    for (LinkDescription remoteLink: remoteLinks) {
      int snid = remoteLink.getSourceNodeId();
      int dnid = remoteLink.getDestinationNodeID();
      
      NodeProxy physNode1 = allocation.getPhysicalNodeHostingLogicalNodeId(snid);
      NodeProxy physNode2 = allocation.getPhysicalNodeHostingLogicalNodeId(dnid);

      CtrlMessage msg = new ConnectRemoteThreadsMsg(snid, dnid, physNode1.getNetworkAddress(), port);
      physNode2.getLink().sendMsgAndRequestAck(msg, AckType.RECEPTION);
      try {
        physNode2.getLink().waitAck(msg);
      } catch (NotAcknowledgeableException e) {
        e.printStackTrace();
      }
      
      CtrlMessage msg2 = new ConnectRemoteThreadsMsg(snid, dnid, physNode2.getNetworkAddress(), port);
      physNode1.getLink().sendMsgAndRequestAck(msg2, AckType.PROCESSING);
      try {
        physNode1.getLink().waitAck(msg2);
      } catch (NotAcknowledgeableException e) {
        e.printStackTrace();
      }
      
      port++;
    }
  }
  
  
  public void start() throws LinkBrokenException
  {
    sendCommandToAllLogicalNodes(ThreadCommandMsg.Command.START, false);
  }
  
  
  public void kill()
  {
    try {
      sendCommandToAllLogicalNodes(ThreadCommandMsg.Command.STOP, true);
    } catch (LinkBrokenException e) { }
    allocation.removeDeadPhysicalNodes(allocation.livePhysicalNodes());
    
    DisposeLog.info(this, "this task has been killed");
  }
  
  
  private void sendCommandToAllLogicalNodes(ThreadCommandMsg.Command cmd, boolean ignoreDead) throws LinkBrokenException
  {
    Set<NodeProxy> pnodes = new HashSet<>(allocation.livePhysicalNodes());
    for (NodeProxy pnode: pnodes) {
      Collection<Integer> lnodes = allocation.logicalNodesHostedInPhysicalNode(pnode);
      CtrlMessage cmsg = new ThreadCommandMsg(lnodes, cmd);
      try {
        pnode.getLink().sendMsg(cmsg);
      } catch (LinkBrokenException e) {
        if (!ignoreDead)
          throw e;
      }
    }
  }
  
  
  public void nodeHasDied(NodeProxy np)
  {
    allocation.removeDeadPhysicalNodes(Collections.singleton(np));
    DisposeLog.critical(this, "RIP node ", np.nodeID());
    
    if (np == owner) {
      DisposeLog.critical(this, "the owner has died; garbage-collecting the rest of the job");
      kill();
    }
  }
}
