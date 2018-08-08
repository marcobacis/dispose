package dispose.net.links;

import java.io.IOException;

import dispose.net.message.CtrlMessage;

public class MonitoredLink
{
  private Link link;
  private Delegate delegate;
  private boolean intentionallyClosed = false;
  
  
  public interface Delegate
  {
    public void messageReceived(CtrlMessage msg);
    public void linkIsBroken(Exception e);
  }
  
  
  public MonitoredLink(Link adaptedLink, Delegate delegate)
  {
    this.link = adaptedLink;
    this.delegate = delegate;
    Thread thd = new Thread(() -> this.monitorThreadMain());
    thd.setName("link-monitor-" + Integer.toHexString(hashCode()));
    thd.start();
  }
  
  
  private void monitorThreadMain()
  {
    try {
      while (true) {
        CtrlMessage m = (CtrlMessage)link.recvMsg(0);
        delegate.messageReceived(m);
      }
    } catch (ClassNotFoundException | IOException e) {
      if (!intentionallyClosed)
        delegate.linkIsBroken(e);
    }
  }
  
  
  public void sendMsg(Object message) throws IOException
  {
    link.sendMsg(message);
  }
  
  
  public void close()
  {
    intentionallyClosed = true;
    link.close();
  }
}
