package dispose.test;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dispose.client.ClientDag;
import dispose.client.Op;
import dispose.client.Stream;
import dispose.client.consumer.StdOutConsumerStream;
import dispose.client.producer.RandomProducerStream;
import dispose.client.producer.SequenceProducerStream;
import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.links.ObjectFifoLink;
import dispose.net.links.SocketLink;
import dispose.net.message.CreateJobMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.JobCommandMsg;
import dispose.net.message.JobCommandMsg.Command;
import dispose.net.node.Node;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class ExampleJob
{
  public static void main(String[] args) throws Exception
  {
    boolean remoteSupervisor = false;
    Node localNode;
    
    if (!remoteSupervisor) {
      Supervisor localSupervisor = new Supervisor();
      Thread supvThread = new Thread(localSupervisor);
      supvThread.start();
      ObjectFifoLink _localLinkA = new ObjectFifoLink();
      ObjectFifoLink _localLinkB = new ObjectFifoLink();
      _localLinkA.connect(_localLinkB);
      localNode = new Node(_localLinkA);
      Thread nodeThread = new Thread(localNode);
      nodeThread.start();
      localSupervisor.registerNode(new NodeProxy(localSupervisor, _localLinkB, NodeProxy.LOCAL_NETWORK_ADDRESS));
      
    } else {
      String lhost = InetAddress.getLocalHost().getHostAddress();
      System.out.println(lhost);
      SocketLink ctrl = SocketLink.connectTo(lhost, Config.nodeCtrlPort);
      localNode = new Node(ctrl);
      Thread nodeThread = new Thread(localNode);
      nodeThread.start();
    }
    
    /* Simple stream example. The final output is a series 
     * of numbers starting from 5.0 and so on.
     */
    Stream source = new SequenceProducerStream(50, 0, 100);
    Stream left = source.apply(Op.MAX, 5);
    Stream right = source.apply(Op.AVG, 3).apply(Op.MAX,5);
    Stream joined = left.join(5, right);
    Stream consumer = new StdOutConsumerStream(joined);
    
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
    
    while (true) {
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
