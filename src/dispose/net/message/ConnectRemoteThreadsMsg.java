package dispose.net.message;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import dispose.log.DisposeLog;
import dispose.net.common.Config;
import dispose.net.links.SocketLink;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.threads.ClosedEndException;

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
  public void executeOnNode(Node node) throws MessageFailureException
  {
    ComputeThread toop = node.getComputeThread(getTo());
    ComputeThread fromop = node.getComputeThread(getFrom());
    
    if (toop != null && fromop != null) {
      throw new MessageFailureException("Both operators are instantiated in the same node");
    }
    
    if (toop == null && fromop == null) {
      throw new MessageFailureException("Operators " + Integer.toString(from) + " and " 
          + Integer.toString(to) + " both don't exist here!");
    }
    
    if (toop != null) {
      setupAcceptSide(toop);
    } else {
      setupConnectSide(fromop);
    }
  }
  
  
  public void setupAcceptSide(ComputeThread toop) throws MessageFailureException
  {
    SocketLink link;
    try {
      link = SocketLink.connectFrom(port());
      toop.addInputLink(link, getFrom());
    } catch (IOException | ClosedEndException e) {
      throw new MessageFailureException(e);
    }
  }
  
  
  public void setupConnectSide(ComputeThread fromop) throws MessageFailureException
  {
    SocketLink link = null;
    boolean success = false;
    int attemptsLeft = 5;
    
    while (!success && attemptsLeft > 0) {
      DisposeLog.info(this, "connection attempt to ", this.host, "; left ", attemptsLeft);
      
      try {
        link = SocketLink.connectTo(getRemoteHost(), port());
        success = true;
        
      } catch (IOException e) {
        DisposeLog.info(this, "failed");
        attemptsLeft--;
        
        if (attemptsLeft > 0) {
          DisposeLog.info(this, "will retry in a while");
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e1) {
            throw new MessageFailureException(e1);
          }
        }
      }
    }
    
    if (!success || link == null) {
      throw new MessageFailureException("connection failed");
    }
    
    try {
      fromop.addOutputLink(link, getTo());
    } catch (ClosedEndException e) {
      throw new MessageFailureException(e);
    }
  }
  
}
