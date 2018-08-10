package dispose.test;

import java.io.IOException;

import dispose.net.common.types.FloatData;
import dispose.net.links.PipeLink;

import dispose.net.node.Operator;
import dispose.net.node.operators.AvgWindowOperator;
import dispose.net.node.operators.MaxWindowOperator;
import dispose.net.node.threads.OperatorThread;


public class OperatorsExerciser
{
  
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
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
   
    OperatorThread maxWrapper = new OperatorThread(max);
    OperatorThread avgWrapper = new OperatorThread(avg);
    
    maxWrapper.addInput(hostMax);
    maxWrapper.addOutput(maxAvgA);
    
    avgWrapper.addInput(maxAvgB);
    avgWrapper.addOutput(avgHost);
    
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
