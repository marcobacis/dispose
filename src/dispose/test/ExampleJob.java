package dispose.test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import dispose.net.links.PipeLink;
import dispose.net.node.Node;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class ExampleJob
{
  public static void main(String[] args) throws IOException, InterruptedException
  {
    Supervisor localSupervisor = new Supervisor();
    Thread supvThread = new Thread(localSupervisor);
    supvThread.start();
    PipeLink _localLinkA = new PipeLink();
    PipeLink _localLinkB = new PipeLink();
    _localLinkA.connect(_localLinkB);
    Node localNode = new Node(_localLinkA);
    Thread nodeThread = new Thread(localNode);
    nodeThread.start();
    localSupervisor.registerNode(new NodeProxy(localSupervisor, _localLinkB));
    
    while (true) {
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
