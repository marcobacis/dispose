package dispose.net.links;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;

public class MonitoredLink
{
  private Link link;
  private Delegate delegate;
  private boolean intentionallyClosed = false;
  private Map<UUID, Boolean> ackStatus = new HashMap<>();
  private ExecutorService msgExecutor;
  
  
  public enum AckType
  {
    RECEPTION,
    PROCESSING
  }
  
  
  public interface Delegate
  {
    public void messageReceived(Message msg) throws MessageFailureException;
    public void linkIsBroken(Exception e);
  }
  
  
  public MonitoredLink(Link adaptedLink, Delegate delegate)
  {
    this.link = adaptedLink;
    this.delegate = delegate;
  }
  
  
  public static void syncMonitorLink(Link link, Delegate delegate)
  {
    MonitoredLink mlink = new MonitoredLink(link, delegate);
    mlink.monitorSynchronously();
  }
  
  
  public static MonitoredLink asyncMonitorLink(Link link, Delegate delegate)
  {
    MonitoredLink mlink = new MonitoredLink(link, delegate);
    Thread thd = new Thread(() -> mlink.monitorSynchronously());
    thd.setName("link-monitor-" + Integer.toHexString(mlink.hashCode()));
    thd.start();
    return mlink;
  }
  
  
  public void monitorSynchronously()
  {
    msgExecutor = Executors.newSingleThreadExecutor();
    
    try {
      while (true) {
        Message m = link.recvMsg(0);
        
        if (m instanceof AckRequestMsg) {
          AckRequestMsg ackreq = (AckRequestMsg) m;
          Message realm = ackreq.getMessage();
          
          if (ackreq.getType() == AckType.RECEPTION) {
            sendMsg(new AckMsg(realm.getUUID()));
            msgExecutor.submit(() -> execMessage(realm, false));
            
          } else if (ackreq.getType() == AckType.PROCESSING) {
            msgExecutor.submit(() -> execMessage(realm, true));
          }
          
        } else if (m instanceof AckMsg) {
          UUID uuid = ((AckMsg) m).getAcknowledgedUUID();
          confirmAck(uuid);
          
        } else {
          msgExecutor.submit(() -> execMessage(m, false));
          
        }
      }
    } catch (LinkBrokenException e) {
      if (!intentionallyClosed)
        delegate.linkIsBroken(e);
      _close();
    }
  }
  
  
  private void execMessage(Message msg, boolean sendAck)
  {
    try {
      delegate.messageReceived(msg);
      if (sendAck)
        sendMsg(new AckMsg(msg.getUUID()));
    } catch (MessageFailureException | LinkBrokenException e) {
      e.printStackTrace();
      _close();
    }
  }
  
  
  private synchronized void confirmAck(UUID uuid)
  {
    ackStatus.put(uuid, true);
    notifyAll();
  }
  
  
  public void sendMsg(Message message) throws LinkBrokenException
  {
    link.sendMsg(message);
  }
  
  
  public synchronized void sendMsgAndRequestAck(Message message) throws LinkBrokenException
  {
    sendMsgAndRequestAck(message, AckType.PROCESSING);
  }
  
  
  public synchronized void sendMsgAndRequestAck(Message message, AckType type) throws LinkBrokenException
  {
    AckRequestMsg ackmsg = new AckRequestMsg(message, type);
    ackStatus.put(message.getUUID(), false);
    link.sendMsg(ackmsg);
  }
  
  
  public synchronized void waitAck(Message sentMessage) throws LinkBrokenException, NotAcknowledgeableException
  {
    if (ackStatus == null)
      throw new LinkBrokenException("closed link");
    
    UUID waituuid = sentMessage.getUUID();
    if (ackStatus.get(waituuid) == null)
      throw new NotAcknowledgeableException("message not previously sent with sendMsgAndRequestAck");
    
    while (ackStatus != null && ackStatus.get(waituuid) == false) {
      try {
        wait();
      } catch (InterruptedException e) {
        delegate.linkIsBroken(e);
        _close();
        throw new LinkBrokenException(e);
      }
    }
    
    if (ackStatus == null)
      throw new LinkBrokenException("the node is down!");
    else
      ackStatus.remove(waituuid);
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
    msgExecutor.shutdown();
  }
}
