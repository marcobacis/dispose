package dispose.net.message;

import java.util.concurrent.TimeUnit;

import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.links.SocketLink;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;

public class ConnectRemoteThreadsMsg extends CtrlMessage
{
  private static final long serialVersionUID = -7142514776117624364L;
  private int from;
  private int to;
  private String host;
  
  private int port;
  
  
  public ConnectRemoteThreadsMsg(int from, int to, String host)
  {
    this.from = from;
    this.to = to;
    this.host = host;
    this.port = Config.nodeOperatorPort;
  }
  
  
  public ConnectRemoteThreadsMsg(int from, int to, String host, int port)
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
    ComputeThread toop = node.getComputeThread(getTo());
    ComputeThread fromop = node.getComputeThread(getFrom());
    
    if (toop != null && fromop != null) {
      throw new Exception("Both operators are instantiated in the same node");
    }
    
    if (toop != null) {
      SocketLink link = SocketLink.connectFrom(port());
      toop.setInputLink(link, getFrom());
      return;
    }
    
    if (fromop != null) {
      SocketLink link = null;
      boolean success = false;
      int attemptsLeft = 5;
      
      while (!success && attemptsLeft > 0) {
        DisposeLog.info(this, "connection attempt to ", this.host, "; left ", attemptsLeft);
        try {
          link = SocketLink.connectTo(getRemoteHost(), port());
          success = true;
        } catch (Exception e) {
          DisposeLog.info(this, "failed");
          attemptsLeft--;
          if (attemptsLeft > 0) {
            DisposeLog.info(this, "will retry in a while");
            TimeUnit.SECONDS.sleep(1);
          }
        }
      }
      
      if (success && link != null)
        fromop.setOutputLink(link, getTo());
      else
        throw new Exception("connection failed");
      return;
    }
    
    throw new Exception("Operators " + Integer.toString(from) + " and " 
          + Integer.toString(to) + " both don't exist here!");
  }
  
}
