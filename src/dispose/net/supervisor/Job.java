
package dispose.net.supervisor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dispose.client.ClientDag;
import dispose.log.DisposeLog;
import dispose.log.LogInfo;
import dispose.net.links.LinkBrokenException;
import dispose.net.links.MonitoredLink.AckType;
import dispose.net.links.NotAcknowledgeableException;
import dispose.net.message.CompletedJobMsg;
import dispose.net.message.ConnectRemoteThreadsMsg;
import dispose.net.message.ConnectThreadsMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.DeployDataSinkThreadMsg;
import dispose.net.message.DeployDataSourceThreadMsg;
import dispose.net.message.DeployOperatorThreadMsg;
import dispose.net.message.ThreadCommandMsg;
import dispose.net.message.chkp.ChkpRequestMsg;
import dispose.net.message.chkp.DeployComputeNodeFromChkpMsg;
import dispose.net.node.ComputeNode;
import dispose.net.node.checkpoint.Checkpoint;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.operators.Operator;
import dispose.net.supervisor.JobDag.LinkDescription;


/** A class representing a job. Not thread-safe. */
public class Job implements LogInfo
{
  private UUID id;
  private Supervisor supervis;
  private NodeProxy owner;
  private JobDag jobDag;
  private JobDagAllocation allocation;
  private CheckpointArchive checkpoints;
  private Status status;


  private enum Status {
    SETUP,
    RUN,
    RECOVERY,
    KILLED
  }


  /* TODO: retry on failure with retry count */

  public Job(UUID id, JobDag jobDag, JobDagAllocation initialNodeAlloc, Supervisor supervis, NodeProxy owner)
  {
    this.id = id;
    this.jobDag = jobDag;
    this.allocation = initialNodeAlloc;
    this.supervis = supervis;
    this.owner = owner;
    this.checkpoints = new CheckpointArchive();
    this.status = Status.SETUP;
  }


  @Override
  public String loggingName()
  {
    return "Job " + id.toString();
  }


  public static Job jobFromClientDag(UUID id, ClientDag dag, Supervisor supervis, NodeProxy owner)
    throws InvalidDagException
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
    materializeNodes(jobDag.getNodes());
    materializeLocalLinks(allocation.localLinks());
    materializeRemoteLinks(allocation.remoteLinks());

    DisposeLog.debug(this, "Job ", id, " materialized");
    DisposeLog.debug(this, allocation.toString());
  }


  private void materializeNodes(Collection<ComputeNode> logNodes) throws LinkBrokenException
  {
    for (ComputeNode logNode : logNodes) {
      NodeProxy physNode = allocation.getPhysicalNodeHostingLogicalNodeId(logNode.getID());
      CtrlMessage msg;
      if (logNode instanceof DataSink) {
        msg = new DeployDataSinkThreadMsg(getID(), (DataSink) logNode);
      } else if (logNode instanceof DataSource) {
        msg = new DeployDataSourceThreadMsg(getID(), (DataSource) logNode);
      } else {
        msg = new DeployOperatorThreadMsg(getID(), (Operator) logNode);
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
    for (LinkDescription localLink : localLinks) {
      NodeProxy physNode = allocation.getPhysicalNodeHostingLogicalNodeId(localLink.getSourceNodeId());
      CtrlMessage msg = new ConnectThreadsMsg(localLink.getSourceNodeId(), localLink.getDestinationNodeID());
      physNode.getLink().sendMsg(msg);
    }
  }


  private void materializeRemoteLinks(Collection<LinkDescription> remoteLinks) throws LinkBrokenException
  {
    /* TODO: parallelize link instantiation */

    int port = 9000;
    for (LinkDescription remoteLink : remoteLinks) {
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
    status = Status.RUN;
    sendCommandToAllLogicalNodes(ThreadCommandMsg.Command.START, false);
  }


  public void completed()
  {
    DisposeLog.info(this, "Completed job " + id);
    CompletedJobMsg msg = new CompletedJobMsg(id);
    try {
      owner.getLink().sendMsgAndRequestAck(msg);
    } catch (LinkBrokenException e) {

    }

    kill();
  }


  public void kill()
  {
    status = Status.KILLED;
    try {
      sendCommandToAllLogicalNodes(ThreadCommandMsg.Command.STOP, true);
    } catch (LinkBrokenException e) {
    }

    allocation.removeDeadPhysicalNodes(allocation.livePhysicalNodes());

    DisposeLog.info(this, "this task has been killed");
    supervis.removeJob(id);
  }


  private void sendCommandToAllLogicalNodes(ThreadCommandMsg.Command cmd, boolean ignoreDead) throws LinkBrokenException
  {
    sendCommandToLogicalNodes(allocation.getLiveLogicalNodes(), cmd, ignoreDead, false);
  }


  private void sendCommandToLogicalNodes(
    Collection<Integer> allowedLNodes, 
    ThreadCommandMsg.Command cmd, 
    boolean ignoreDead,
    boolean waitAcknowledgment) throws LinkBrokenException
  {
    Set<NodeProxy> pnodes = new HashSet<>(allocation.livePhysicalNodes());
    for (NodeProxy pnode : pnodes) {
      Collection<Integer> lnodes = allocation.logicalNodesHostedInPhysicalNode(pnode);
      Set<Integer> restLNodes = new HashSet<>(lnodes);
      restLNodes.retainAll(allowedLNodes);
      if (restLNodes.isEmpty())
        continue;

      CtrlMessage cmsg = new ThreadCommandMsg(lnodes, cmd);
      try {
        if (waitAcknowledgment) {
          pnode.getLink().sendMsgAndRequestAck(cmsg);
          pnode.getLink().waitAck(cmsg);
        } else {
          pnode.getLink().sendMsg(cmsg);
        }
      } catch (LinkBrokenException e) {
        if (!ignoreDead)
          throw e;
      } catch (NotAcknowledgeableException e) {
        throw new RuntimeException("wat");
      }
    }
  }


  public void nodeHasDied(NodeProxy np)
  {
    allocation.removeDeadPhysicalNodes(Collections.singleton(np));
    DisposeLog.critical(this, "RIP node ", Integer.toHexString(np.nodeID()));

    if (np == owner) {
      DisposeLog.critical(this, "the owner has died; garbage-collecting the rest of the job");
      kill();
      return;
    }

    if (status == Status.RECOVERY)
      return;
    status = Status.RECOVERY;

    int srcid = jobDag.getSourceNodeId();
    Set<Integer> otherNodes = allocation.getLiveLogicalNodes(false, true);

    DisposeLog.critical(this, "suspending live nodes");
    try {
      sendCommandToLogicalNodes(Collections.singleton(srcid), ThreadCommandMsg.Command.SUSPEND, false, true);
    } catch (LinkBrokenException e) {
      DisposeLog.error(this, "cannot suspend source; exc = ", e);
      return;
    }
    try {
      sendCommandToLogicalNodes(otherNodes, ThreadCommandMsg.Command.SUSPEND, false, false);
    } catch (LinkBrokenException e) {
      DisposeLog.error(this, "cannot suspend live nodes; exc = ", e);
      return;
    }
    
    DisposeLog.critical(this, "waiting 5 seconds to let the links flush themselves");
    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) { }
    
    attemptRecovery();
  }


  private void attemptRecovery()
  {
    DisposeLog.critical(this, "attempting a recover");

    UUID ckp = checkpoints.getLatestCompleteCheckpointId(jobDag);
    if (ckp == null) {
      DisposeLog.error(this, "no eligible checkpoint found");
      return;
    }
    DisposeLog.critical(this, "will use checkpoint ", ckp);

    Set<LinkDescription> linksAlreadyAlive = allocation.getLiveLinks();

    try {
      allocation.allocateMissingNodes(supervis.getNodes(), owner);
    } catch (ResourceUnderrunException e) {
      DisposeLog.critical(this, "recover failure; not enough nodes left");
      return;
    }

    try {
      restoreStateFromCheckpoint(jobDag.getNodes(), ckp);
    } catch (LinkBrokenException e1) {
      DisposeLog.error("could not restore checkpoint", ckp, "; exc = ", e1);
      return;
    }

    Set<LinkDescription> newLocalLinks = new HashSet<>(allocation.localLinks());
    newLocalLinks.removeAll(linksAlreadyAlive);
    Set<LinkDescription> newRemoteLinks = new HashSet<>(allocation.remoteLinks());
    newRemoteLinks.removeAll(linksAlreadyAlive);

    DisposeLog.critical(this, "rebuilding data overlay network");
    try {
      materializeLocalLinks(newLocalLinks);
      materializeRemoteLinks(newRemoteLinks);
    } catch (LinkBrokenException e) {
      DisposeLog.critical(this, "could not restore links; exc = ", e);
      return;
    }

    DisposeLog.critical(this, "resuming processing");
    try {
      Set<Integer> otherNodes = allocation.getLiveLogicalNodes(false, true);
      int sourceId = jobDag.getSourceNodeId();
      sendCommandToLogicalNodes(otherNodes, ThreadCommandMsg.Command.RESUME, false, true);
      sendCommandToLogicalNodes(Collections.singleton(sourceId), ThreadCommandMsg.Command.RESUME, false, true);
      status = Status.RUN;
    } catch (LinkBrokenException e) {
      DisposeLog.critical(this, "could not restart job after restore; exc = ", e);
    }

    DisposeLog.critical(this, "recovery completed");
  }


  public void requestCheckpoint()
  {
    DisposeLog.info(this, "requesting checkpoint");
    UUID newckpid = UUID.randomUUID();
    checkpoints.addNewCheckpoint(newckpid);
    try {
      owner.getLink().sendMsg(new ChkpRequestMsg(newckpid));
    } catch (LinkBrokenException e) {
      DisposeLog.error(this, "cannot request checkpoint; exc = ", e);
    }
  }


  public boolean reclaimCheckpointPart(UUID ckpid, Checkpoint part)
  {
    if (!checkpoints.containsCheckpoint(ckpid))
      return false;

    checkpoints.addCheckpointPart(ckpid, part);
    DisposeLog.info(this, "reclaimed ckp ", ckpid, " part opid=", part.getComputeNode().getID());
    return true;
  }


  private void restoreStateFromCheckpoint(Collection<ComputeNode> logNodes, UUID ckpid) throws LinkBrokenException
  {
    DisposeLog.critical(this, "restoring node state");
    
    for (ComputeNode logNode : logNodes) {
      NodeProxy physNode = allocation.getPhysicalNodeHostingLogicalNodeId(logNode.getID());

      Checkpoint ckp = checkpoints.getCheckpointPart(ckpid, logNode.getID());
      if (ckp == null)
        /* sink */
        continue;

      CtrlMessage msg = new DeployComputeNodeFromChkpMsg(ckpid, ckp);
      physNode.getLink().sendMsgAndRequestAck(msg);
      // TODO: wait acks after sending all the messages
      try {
        physNode.getLink().waitAck(msg);
      } catch (NotAcknowledgeableException e) {
        e.printStackTrace();
      }
    }
  }
}
