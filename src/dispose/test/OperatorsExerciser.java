package dispose.test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dispose.net.common.types.FloatData;
import dispose.net.links.ObjectFifoLink;
import dispose.net.node.Node;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.MaxWindowOperator;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.OperatorThread;


public class OperatorsExerciser
{
  
  public static void main(String[] args) throws Exception
  {
    ObjectFifoLink to = new ObjectFifoLink();
    ObjectFifoLink hostMax = new ObjectFifoLink();
    to.connect(hostMax);
    
    ObjectFifoLink maxAvgA = new ObjectFifoLink();
    ObjectFifoLink maxAvgB = new ObjectFifoLink();
    maxAvgA.connect(maxAvgB);
    
    ObjectFifoLink avgHost = new ObjectFifoLink();
    ObjectFifoLink from  = new ObjectFifoLink();
    avgHost.connect(from);
    
    // here -> max(3,1) -> avg(1,1) -> here
    
    Operator max = new MaxWindowOperator(1, 3, 1);
    
    Operator avg = new AvgWindowOperator(2, 2, 2);
    
    Node node = new Node(null);
  
    UUID jid = UUID.randomUUID();
    
    OperatorThread maxWrapper = new OperatorThread(node, jid, max);
    OperatorThread avgWrapper = new OperatorThread(node, jid, avg);
    
    maxWrapper.addInputLink(hostMax, 0);
    maxWrapper.addOutputLink(maxAvgA, avg.getID());
    
    avgWrapper.addInputLink(maxAvgB, max.getID());
    avgWrapper.addOutputLink(avgHost, 0);
    
    maxWrapper.start();
    avgWrapper.start();
    
    for (int i = 0; i < 15; i++) {
      to.sendMsg(new FloatData(-1, i));
    }

    for (int i=0; i<6; i++) {
      FloatData fd = (FloatData) from.recvMsg();
      System.out.println(fd + "; " + fd.getTimestamp() + "; " + fd.getUUID());
    }
    
    TimeUnit.SECONDS.sleep(3);
    
    maxAvgA.close();
    System.out.println("maxAvgA closed");
    
    TimeUnit.SECONDS.sleep(3);
    
    maxAvgB.close();
    System.out.println("maxAvgB closed");
    
    TimeUnit.SECONDS.sleep(3);
    
    maxWrapper.pause();
    avgWrapper.pause();
    
    ObjectFifoLink maxAvgA1 = new ObjectFifoLink();
    ObjectFifoLink maxAvgB1 = new ObjectFifoLink();
    maxAvgA1.connect(maxAvgB1);
    
    maxWrapper.addOutputLink(maxAvgA1, avg.getID());
    avgWrapper.addInputLink(maxAvgB1, max.getID());
    
    maxWrapper.resume();
    avgWrapper.resume();
    
    for (int i = 0; i < 15; i++) {
      to.sendMsg(new FloatData(-1, i));
    }

    for (int i=0; i<6; i++) {
      FloatData fd = (FloatData) from.recvMsg();
      System.out.println(fd + "; " + fd.getTimestamp() + "; " + fd.getUUID());
    }
    
    maxWrapper.stop();
    avgWrapper.stop();
  }

}
