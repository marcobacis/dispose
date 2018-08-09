package dispose.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import dispose.net.common.Config;
import dispose.net.common.types.FloatData;
import dispose.net.links.PipeLink;
import dispose.net.links.SocketLink;
import dispose.net.message.ConnectOperatorMsg;
import dispose.net.message.ConnectRemoteOperatorMsg;
import dispose.net.message.DeployOperatorMsg;
import dispose.net.message.StartOperatorMsg;
import dispose.net.node.Node;
import dispose.net.node.Operator;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.MaxWindowOperator;


public class NodeExerciser
{
  public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException
  {
    //SocketLink ctrl = SocketLink.connectFrom(Config.nodeCtrlPort);
    PipeLink ctrlA = new PipeLink();
    PipeLink ctrlB = new PipeLink();
    ctrlA.connect(ctrlB);
    Node node = new Node(ctrlB);
    Thread nthd = new Thread(node);
    nthd.start();
    
    // here -> max(3,1) -> avg(1,1) -> here
    
    Operator max = new MaxWindowOperator(1, 3, 1);
    
    Operator avg = new AvgWindowOperator(2, 2, 2);
    
    ctrlA.sendMsg(new DeployOperatorMsg(max));
    ctrlA.sendMsg(new DeployOperatorMsg(avg));
    
    ctrlA.sendMsg(new ConnectOperatorMsg(max.getID(), avg.getID()));
    
    ctrlA.sendMsg(new ConnectRemoteOperatorMsg(0, max.getID(), "127.0.0.1", 9002));
    
    TimeUnit.SECONDS.sleep(5);
    
    SocketLink to = SocketLink.connectTo("127.0.0.1", 9002);
    
    ctrlA.sendMsg(new ConnectRemoteOperatorMsg(avg.getID(), 0, "127.0.0.1",9003));
    
    SocketLink from = SocketLink.connectFrom(9003);
    
    ctrlA.sendMsg(new StartOperatorMsg());
    
    TimeUnit.SECONDS.sleep(3);
    
    for(int i = 0; i < 15; i++) {
      to.sendMsg(new FloatData(i));
    }
    
    to.getOutputStream().flush();
    
    while(true) {
      FloatData fd = (FloatData)from.recvMsg();
      System.out.println(fd + "; " + fd.getTimestamp() + "; " + fd.getUUID());
    }
  }
}
