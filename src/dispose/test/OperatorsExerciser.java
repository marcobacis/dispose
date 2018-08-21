package dispose.test;

import java.util.UUID;

import dispose.net.common.types.FloatData;
import dispose.net.links.PipeLink;
import dispose.net.node.Node;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.MaxWindowOperator;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.OperatorThread;


public class OperatorsExerciser
{
  
  public static void main(String[] args) throws Exception
  {

    PipeLink to = new PipeLink();
    PipeLink hostMax = new PipeLink();
    to.connect(hostMax);
    
    PipeLink maxAvgA = new PipeLink();
    PipeLink maxAvgB = new PipeLink();
    maxAvgA.connect(maxAvgB);
    
    PipeLink avgHost = new PipeLink();
    PipeLink from  = new PipeLink();
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
    
    for(int i = 0; i < 15; i++) {
      to.sendMsg(new FloatData(i));
    }

    
    while(true) {
      FloatData fd = (FloatData) from.recvMsg();
      System.out.println(fd + "; " + fd.getTimestamp() + "; " + fd.getUUID());
    }
  }

}
