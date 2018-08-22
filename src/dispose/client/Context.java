
package dispose.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.UUID;

import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.links.Link;
import dispose.net.links.LinkBrokenException;
import dispose.net.links.NotAcknowledgeableException;
import dispose.net.links.ObjectFifoLink;
import dispose.net.links.SocketLink;
import dispose.net.message.CreateJobMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.JobCommandMsg;
import dispose.net.message.JobCommandMsg.Command;
import dispose.net.node.Node;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;
import dispose.test.ExampleJob;


public class Context
{
  private boolean remoteSup = false;
  private String remoteSupervisorAddress = "";

  private boolean ready = false;

  private Node localNode;
  private Supervisor localSupervisor;
  private Thread localSupervisorThread;
  private Thread localNodeThread;
  private Link ctrlLink;


  public Context()
  {
    remoteSup = false;

    try {
      initializeLocal();
      ready = true;
    } catch (LinkBrokenException e) {
      ready = false;
      DisposeLog.critical(this, "Error during context initialization: ", e);
    }

  }


  public Context(String supervisorAddress)
  {
    try {
      remoteSup = true;
      remoteSupervisorAddress = supervisorAddress;

      initializeRemote();

      ready = true;

    } catch (IOException e) {
      ready = false;
      DisposeLog.critical(this, "Error during context initialization: ", e);
    }
  }


  private void initializeRemote() throws UnknownHostException, IOException
  {
    remoteSup = true;

    System.out.println(remoteSupervisorAddress);
    ctrlLink = SocketLink.connectTo(remoteSupervisorAddress, Config.nodeCtrlPort);
    localNode = new Node(ctrlLink);
    localNodeThread = new Thread(localNode);
    localNodeThread.start();
  }


  private void initializeLocal() throws LinkBrokenException
  {
    remoteSup = false;

    localSupervisor = new Supervisor();
    localSupervisorThread = new Thread(localSupervisor);
    localSupervisorThread.setName("local-supervisor");
    localSupervisorThread.start();
    ObjectFifoLink _localLinkA = new ObjectFifoLink();
    ObjectFifoLink _localLinkB = new ObjectFifoLink();
    _localLinkA.connect(_localLinkB);

    ctrlLink = _localLinkA;

    localNode = new Node(ctrlLink);
    localNodeThread = new Thread(localNode);
    localNodeThread.setName("local-node");
    localNodeThread.start();
    localSupervisor.registerNode(new NodeProxy(localSupervisor, _localLinkB, NodeProxy.LOCAL_NETWORK_ADDRESS));
  }


  public void run(Stream consumer) throws LinkBrokenException, NotAcknowledgeableException, InterruptedException
  {
    if (!ready) {
      DisposeLog.critical(this, "The job is not ready due to errors. Restart the application to try again.");
      return;
    }

    ClientDag compDag = ClientDag.derive(consumer);

    UUID jobid = UUID.randomUUID();
    CtrlMessage newmsg = new CreateJobMsg(jobid, compDag);
    localNode.getControlLink().sendMsgAndRequestAck(newmsg);
    localNode.getControlLink().waitAck(newmsg);

    CtrlMessage idmsg = new JobCommandMsg(jobid, Command.START);
    localNode.getControlLink().sendMsgAndRequestAck(idmsg);
    localNode.getControlLink().waitAck(idmsg);

    DisposeLog.info(ExampleJob.class, "the dag has been instantiated!");
    DisposeLog.debug(ExampleJob.class, compDag);

    localNode.waitJobCompleted(jobid);

    // TODO if not remote, wait for the local supervisor thread to die
    // other TODO : check that the local node thread joins!
    if (!remoteSup) {
      // something to stop the supervisor goes here
      localSupervisorThread.join();
    }
    localNodeThread.join();

    DisposeLog.info(this, "The End");
  }

}
