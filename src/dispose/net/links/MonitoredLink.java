package dispose.net.links;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import dispose.net.message.CtrlMessage;

public class MonitoredLink
{
  private Link link;
  private Delegate delegate;
  private boolean intentionallyClosed = false;
  private Map<UUID, Boolean> ackStatus = new HashMap<>();
  
  
  public interface Delegate
  {
    public void messageReceived(CtrlMessage msg) throws Exception;
    public void linkIsBroken(Exception e);
  }
  
  
  private MonitoredLink(Link adaptedLink, Delegate delegate)
  {
    this.link = adaptedLink;
    this.delegate = delegate;
  }
  
  
  public static void syncMonitorLink(Link link, Delegate delegate)
  {
    MonitoredLink mlink = new MonitoredLink(link, delegate);
    mlink.monitorThreadMain();
  }
  
  
  public static MonitoredLink asyncMonitorLink(Link link, Delegate delegate)
  {
    MonitoredLink mlink = new MonitoredLink(link, delegate);
    Thread thd = new Thread(() -> mlink.monitorThreadMain());
    thd.setName("link-monitor-" + Integer.toHexString(mlink.hashCode()));
    thd.start();
    return mlink;
  }
  
  
  private void monitorThreadMain()
  {
    try {
      while (true) {
        CtrlMessage m = (CtrlMessage)link.recvMsg(0);
        
        if (m instanceof AckRequestMsg) {
          CtrlMessage realm = ((AckRequestMsg) m).getMessage();
          delegate.messageReceived(realm);
          sendMsg(new AckMsg(realm.getUUID()));
          
        } else if (m instanceof AckMsg) {
          UUID uuid = ((AckMsg) m).getAcknowledgedUUID();
          confirmAck(uuid);
          
        } else {
          delegate.messageReceived(m);
        }
      }
    } catch (SocketException e) {
      if (!intentionallyClosed)
        delegate.linkIsBroken(e);
      _close();
    } catch (Exception e) {
      delegate.linkIsBroken(e);
      _close();
    }
  }
  
  
  private synchronized void confirmAck(UUID uuid)
  {
    ackStatus.put(uuid, true);
    notifyAll();
  }
  
  
  public void sendMsg(CtrlMessage message) throws IOException
  {
    link.sendMsg(message);
  }
  
  
  public synchronized void sendMsgAndRequestAck(CtrlMessage message) throws IOException
  {
    AckRequestMsg ackmsg = new AckRequestMsg(message);
    ackStatus.put(message.getUUID(), false);
    link.sendMsg(ackmsg);
  }
  
  
  public synchronized void waitAck(CtrlMessage sentMessage) throws Exception
  {
    UUID waituuid = sentMessage.getUUID();
    if (waituuid == null)
      throw new Exception("message not previously sent with sendMsgAndRequestAck");
    
    while (ackStatus != null && ackStatus.get(waituuid) == false) {
      try {
        wait();
      } catch (InterruptedException e) {
        // usually never happens
        e.printStackTrace();
      }
    }
    
    if (ackStatus == null)
      throw new Exception("the node is down!");
  }
  
  
  public synchronized void close()
  {
    intentionallyClosed = true;
    _close();
  }
  
  
  private synchronized void _close()
  {
    link.close();
    ackStatus = null;
    notifyAll();
  }
}
