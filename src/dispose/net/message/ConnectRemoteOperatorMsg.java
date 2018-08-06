package dispose.net.message;

import dispose.net.common.Config;
import dispose.net.links.SocketLink;
import dispose.net.node.Node;
import dispose.net.node.OperatorThread;

public class ConnectRemoteOperatorMsg implements CtrlMessage
{
  private static final long serialVersionUID = -7142514776117624364L;
  private int from;
  private int to;
  private String host;
  
  private int port;
  
  
  public ConnectRemoteOperatorMsg(int from, int to, String host)
  {
    this.from = from;
    this.to = to;
    this.host = host;
    this.port = Config.nodeOperatorPort;
  }
  
  
  public ConnectRemoteOperatorMsg(int from, int to, String host, int port)
  {
    this(from, to, host);
    this.port = port;
  }
  
  
  public int getFrom()
  {
    return this.from;
  }
  
  
  public int getTo()
  {
    return this.to;
  }
  
  
  public String getRemoteHost()
  {
    return this.host;
  }
  
  
  public int port()
  {
    return this.port;
  }

  
  @Override
  public void executeOnNode(Node node) throws Exception
  {
    OperatorThread fromop = node.getOperator(getFrom());
    
    if (fromop != null) {
      SocketLink link = SocketLink.connectTo(getRemoteHost(), port()); 
      fromop.addOutput(link);
      return;
    }
    
    OperatorThread toop = node.getOperator(getTo());
    if (toop == null)
      throw new Exception("Operators " + Integer.toString(from) + " and " 
          + Integer.toString(to) + " both don't exist here!");
    
    SocketLink link = SocketLink.connectFrom(port());
    toop.addInput(link);
  }
  
}
