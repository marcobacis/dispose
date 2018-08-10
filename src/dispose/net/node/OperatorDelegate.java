package dispose.net.node;

import dispose.net.common.DataAtom;
import dispose.net.common.types.FloatData;
import dispose.net.links.MonitoredLink.Delegate;
import dispose.net.message.Message;

public class OperatorDelegate implements Delegate
{

  OperatorThread op;
  int StreamIndex;
  
  
  public OperatorDelegate(OperatorThread op, int idx)
  {
    this.op = op;
    this.StreamIndex = idx;
  }
  
  @Override
  public void messageReceived(Message msg) throws Exception
  {
    
    if(msg instanceof DataAtom) {
      System.out.println("Message received -> " + ((FloatData) msg).floatValue());
      this.op.notifyElement(StreamIndex, ((DataAtom) msg));
      this.op.process();
    }
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    System.out.println("Fuck the " + this.StreamIndex + "th link on operator " + op.getID() + " is broken");
  }

}
  