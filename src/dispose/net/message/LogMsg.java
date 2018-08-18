package dispose.net.message;

import dispose.log.DisposeLog;
import dispose.net.node.Node;

public class LogMsg extends CtrlMessage
{
  private static final long serialVersionUID = -2139442300869762386L;
  private String origin;
  private String message;
  
  
  public LogMsg(String origin, String message)
  {
    this.message = message;
    this.origin = origin;
  }
  

  @Override
  public void executeOnNode(Node node)
  {
    DisposeLog.info(this, "[", origin, "] ", message);
  }
}
