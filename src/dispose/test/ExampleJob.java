package dispose.test;

import java.util.concurrent.TimeUnit;

import dispose.client.ClientDag;
import dispose.client.FileConsumerStream;
import dispose.client.FileProducerStream;
import dispose.client.Op;
import dispose.client.Stream;
import dispose.net.links.ObjectFifoLink;
import dispose.net.message.InstantiateDagMsg;
import dispose.net.message.StartThreadMsg;
import dispose.net.node.Node;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class ExampleJob
{
  public static void main(String[] args) throws Exception
  {
    Supervisor localSupervisor = new Supervisor();
    Thread supvThread = new Thread(localSupervisor);
    supvThread.start();
    ObjectFifoLink _localLinkA = new ObjectFifoLink();
    ObjectFifoLink _localLinkB = new ObjectFifoLink();
    _localLinkA.connect(_localLinkB);
    Node localNode = new Node(_localLinkA);
    Thread nodeThread = new Thread(localNode);
    nodeThread.start();
    localSupervisor.registerNode(new NodeProxy(localSupervisor, _localLinkB, NodeProxy.LOCAL_NETWORK_ADDRESS));
    
    Stream source = new FileProducerStream("ciao.csv");
    Stream a = source.apply(Op.MAX, 5);
    Stream b = source.apply(Op.AVG, 5);
    Stream d = b.join(3, source.apply(Op.MIN, 5)).apply(Op.MIN, 4);
    Stream consumer = new FileConsumerStream("output.csv", a.join(4, d, b).apply(Op.MAX, 1));
    ClientDag compDag = ClientDag.derive(consumer);
    
    CtrlMessage idmsg = new InstantiateDagMsg(compDag);
    localNode.getControlLink().sendMsgAndRequestAck(idmsg);
    localNode.getControlLink().waitAck(idmsg);
    System.out.println("the dag has been instantiated!");
    localNode.getControlLink().sendMsg(new StartThreadMsg());
    
    while (true) {
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
