package dispose.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import dispose.net.common.Config;
import dispose.net.common.types.FloatData;
import dispose.net.links.MonitoredLink;
import dispose.net.links.ObjectFifoLink;
import dispose.net.links.PipeLink;
import dispose.net.links.SocketLink;
import dispose.net.message.ConnectThreadsMsg;
import dispose.net.message.DeployDataSinkThreadMsg;
import dispose.net.message.DeployDataSourceThreadMsg;
import dispose.net.message.ConnectRemoteThreadsMsg;
import dispose.net.message.DeployOperatorThreadMsg;
import dispose.net.message.StartThreadMsg;
import dispose.net.node.Node;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.datasinks.ObjectLogDataSink;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.datasources.RandomFloatDataSrc;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.MaxWindowOperator;
import dispose.net.node.operators.Operator;


public class NodeExerciser
{
  public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException
  {
    ObjectFifoLink ctrl0A = new ObjectFifoLink();
    ObjectFifoLink ctrl0B = new ObjectFifoLink();
    ctrl0A.connect(ctrl0B);
    Node node0 = new Node(ctrl0B);
    Thread nthd0 = new Thread(node0);
    nthd0.start();
    
    ObjectFifoLink ctrl1A = new ObjectFifoLink();
    ObjectFifoLink ctrl1B = new ObjectFifoLink();
    ctrl1A.connect(ctrl1B);
    Node node1 = new Node(ctrl1B);
    Thread nthd1 = new Thread(node1);
    nthd1.start();
    
    // NODE-0[ randomNumbers() -> max(3,1) ] -socket-> NODE-1[ avg(1,1) -> printer ]
    
    DataSource rand = new RandomFloatDataSrc(100, 1000);
    Operator max = new MaxWindowOperator(1, 3, 1);
    Operator avg = new AvgWindowOperator(2, 2, 2);
    DataSink printer = new ObjectLogDataSink(101);
    
    ctrl0A.sendMsg(new DeployDataSourceThreadMsg(rand));
    ctrl0A.sendMsg(new DeployOperatorThreadMsg(max));
    ctrl1A.sendMsg(new DeployOperatorThreadMsg(avg));
    ctrl1A.sendMsg(new DeployDataSinkThreadMsg(printer));
    
    ctrl0A.sendMsg(new ConnectThreadsMsg(rand.getID(), max.getID()));
    ctrl0A.sendMsg(new ConnectRemoteThreadsMsg(max.getID(), avg.getID(), "127.0.0.1", 9003));
    ctrl1A.sendMsg(new ConnectRemoteThreadsMsg(max.getID(), avg.getID(), "127.0.0.1", 9003));
    ctrl1A.sendMsg(new ConnectThreadsMsg(avg.getID(), printer.getID()));
    
    ctrl0A.sendMsg(new StartThreadMsg());
    ctrl1A.sendMsg(new StartThreadMsg());

    while (true) {
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
