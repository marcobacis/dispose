package dispose.net.message;

import dispose.net.node.Node;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.threads.SinkThread;

public class DeployDataSinkThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = 2436961482738692188L;
  private DataSink dataSink;
  
  
  public DeployDataSinkThreadMsg(DataSink dataSink)
  {
    this.dataSink = dataSink;
  }
  
  
  @Override
  public void executeOnNode(Node node) throws Exception
  {
    SinkThread dst = new SinkThread(dataSink);
    node.addComputeThread(dst.getID(), dst);
  }
}
