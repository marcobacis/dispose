package dispose.net.message;

import java.util.UUID;

import dispose.net.node.Node;
import dispose.net.node.datasinks.DataSink;
import dispose.net.node.threads.SinkThread;

public class DeployDataSinkThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = 2436961482738692188L;
  private DataSink dataSink;
  private UUID jid;
  
  
  public DeployDataSinkThreadMsg(UUID jid, DataSink dataSink)
  {
    this.jid = jid;
    this.dataSink = dataSink;
  }
  
  
  @Override
  public void executeOnNode(Node node)
  {
    SinkThread dst = new SinkThread(node, jid, dataSink);
    node.addComputeThread(dst.getID(), dst);
  }
}
