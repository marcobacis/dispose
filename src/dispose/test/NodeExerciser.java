package dispose.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import dispose.net.common.Config;
import dispose.net.common.types.FloatData;
import dispose.net.links.MonitoredLink;
import dispose.net.links.PipeLink;
import dispose.net.links.SocketLink;
import dispose.net.message.ConnectThreadsMsg;
import dispose.net.message.ConnectRemoteThreadsMsg;
import dispose.net.message.DeployOperatorThreadMsg;
import dispose.net.message.StartThreadMsg;
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
    
    ctrlA.sendMsg(new DeployOperatorThreadMsg(max));
    ctrlA.sendMsg(new DeployOperatorThreadMsg(avg));
    
    ctrlA.sendMsg(new ConnectThreadsMsg(max.getID(), avg.getID()));
    
    ctrlA.sendMsg(new ConnectRemoteThreadsMsg(0, max.getID(), "127.0.0.1", 9002));
    
    TimeUnit.SECONDS.sleep(5);
    
    SocketLink to = SocketLink.connectTo("127.0.0.1", 9002);
    
    ctrlA.sendMsg(new ConnectRemoteThreadsMsg(avg.getID(), 0, "127.0.0.1",9003));
    
    SocketLink from = SocketLink.connectFrom(9003);
    
    ctrlA.sendMsg(new StartThreadMsg());
    
    for(int i = 0; i < 15; i++) {
      to.sendMsg(new FloatData(i));
    }
        
    while(true) {
      FloatData fd = (FloatData) from.recvMsg();
      System.out.println(fd + "; " + fd.getTimestamp() + "; " + fd.getUUID());
    }
  }
}
