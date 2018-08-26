package dispose.net.links;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dispose.log.DisposeLog;
import dispose.log.LogInfo;
import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;

public class MonitoredLink
{
  private Link link;
  private Delegate delegate;
  private boolean intentionallyClosed = false;
  private Map<UUID, Boolean> ackStatus = new HashMap<>();
  private ExecutorService msgExecutor;
  private Timer heartbeatTimer;
  private int timeout = 0;
  
  
  public enum AckType
  {
    RECEPTION,
    PROCESSING
  }
  
  
  public interface Delegate
  {
    public void messageReceived(Message msg) throws MessageFailureException;
    
    /** Method called by MonitoredLink when the link being monitored is no longer
     * working.
     * @param e The exception which caused the link to break or null if the timeout
     * expired.*/
    public void linkIsBroken(Exception e);
  }
  
  
  public MonitoredLink(Link adaptedLink, Delegate delegate)
  {
    this.link = adaptedLink;
    this.delegate = delegate;
  }
  
  
  public static void syncMonitorLink(Link link, Delegate delegate, int timeout)
  {
    MonitoredLink mlink = new MonitoredLink(link, delegate);
    mlink.setTimeoutPeriod(timeout);
    mlink.monitorSynchronously();
  }
  
  
  public static MonitoredLink asyncMonitorLink(Link link, Delegate delegate, int timeout)
  {
    MonitoredLink mlink = new MonitoredLink(link, delegate);
    mlink.setTimeoutPeriod(timeout);
    
    Thread thd = new Thread(() -> mlink.monitorSynchronously());
    String thdlabel;
    if (delegate instanceof LogInfo) {
      thdlabel = ((LogInfo)delegate).loggingName().toLowerCase().replaceAll("\\s", "-");
    } else {
      thdlabel = Integer.toHexString(mlink.hashCode());
    }
    thd.setName("link-monitor-" + thdlabel);
    thd.start();
    
    return mlink;
  }
  
  
  public void monitorSynchronously()
  {
    msgExecutor = Executors.newSingleThreadExecutor();
    
    try {
      while (true) {
        Message m = link.recvMsg(timeout);
        if (m == null)
          break;
        
        if (m instanceof HeartbeatMsg) {
          DisposeLog.info(this, "heartbeat from ", link);
          
        } else if (m instanceof AckRequestMsg) {
          AckRequestMsg ackreq = (AckRequestMsg) m;
          Message realm = ackreq.getMessage();
          
          if (ackreq.getType() == AckType.RECEPTION) {
            link.sendMsg(new AckMsg(realm.getUUID()));
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
      
      delegate.linkIsBroken(null);
      _close();
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
  
  
  public synchronized void sendMsg(Message message) throws LinkBrokenException
  {
    link.sendMsg(message);
  }
  
  
  public synchronized void sendMsgAndRequestAck(Message message) throws LinkBrokenException
  {
    sendMsgAndRequestAck(message, AckType.PROCESSING);
  }
  
  
  public synchronized void sendMsgAndRequestAck(Message message, AckType type) throws LinkBrokenException
  {
    if (ackStatus == null)
      throw new LinkBrokenException("closed link");
    
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
    setHeartbeatSendPeriod(Integer.MAX_VALUE);
    link.close();
    ackStatus = null;
    notifyAll();
    msgExecutor.shutdown();
  }
  
  
  private class HeartbeatTask extends TimerTask
  {
    MonitoredLink parent;
    
    @Override
    public void run()
    {
      parent.msgExecutor.submit(() -> {
        try {
          parent.sendMsg(new HeartbeatMsg());
        } catch (LinkBrokenException e) { }
      });
    }
  }
  
  
  public synchronized void setHeartbeatSendPeriod(int msec)
  {
    if (heartbeatTimer != null) {
      heartbeatTimer.cancel();
    }
    if (msec == Integer.MAX_VALUE) {
      return;
    }
    HeartbeatTask hbt = new HeartbeatTask();
    hbt.parent = this;
    
    heartbeatTimer = new Timer("heartbeat-" + Integer.toHexString(hashCode()));
    heartbeatTimer.schedule(hbt, (int)(Math.random() * 0.25 * msec), msec);
  }
  
  
  public synchronized void setTimeoutPeriod(int msec)
  {
    this.timeout = msec; 
  }
}
