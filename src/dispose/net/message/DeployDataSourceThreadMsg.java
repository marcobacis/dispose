package dispose.net.message;

import dispose.net.node.Node;
import dispose.net.node.datasources.DataSource;
import dispose.net.node.threads.SourceThread;

public class DeployDataSourceThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = 2436961482738692188L;
  private DataSource dataSource;
  
  
  public DeployDataSourceThreadMsg(DataSource dataSource)
  {
    this.dataSource = dataSource;
  }
  
  
  @Override
  public void executeOnNode(Node node)
  {
    SourceThread dst = new SourceThread(node, dataSource);
    node.addComputeThread(dst.getID(), dst);
  }
}
