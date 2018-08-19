package dispose.net.links;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import dispose.net.message.Message;

public class ObjectFifoLink implements Link
{
  private BlockingQueue<Message> toOther = new ArrayBlockingQueue<>(8);
  private BlockingQueue<Message> fromOther;
  
  
  public void connect(ObjectFifoLink oppositeLink)
  {
    oppositeLink.fromOther = toOther;
    fromOther = oppositeLink.toOther;
  }
  
  
  @Override
  public void sendMsg(Message message) throws LinkBrokenException
  {
    if (toOther == null)
      throw new LinkBrokenException("link closed");
    
    try {
      toOther.put(message);
    } catch (InterruptedException e) {
      close();
      throw new LinkBrokenException(e);
    }
  }


  @Override
  public Message recvMsg() throws LinkBrokenException
  {
    return recvMsg(0);
  }


  @Override
  public Message recvMsg(int timeoutms) throws LinkBrokenException
  {
    if (fromOther == null)
      throw new LinkBrokenException("link closed");
    
    Message msg;

    try {
      if (timeoutms == 0)
        msg = fromOther.take();
      else
        msg = fromOther.poll(timeoutms, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      close();
      throw new LinkBrokenException(e);
    }
    
    if (msg instanceof Poison) {
      close();
      throw new LinkBrokenException("link closed");
    }
    
    return msg;
  }
  
  
  private class Poison extends Message
  {
    private static final long serialVersionUID = -5901853992080163917L;

    
    @Override
    public UUID getUUID()
    {
      return null;
    }
  }


  @Override
  public void close()
  {
    if (toOther != null) {
      try {
        toOther.put(new Poison());
      } catch (InterruptedException e) { }
    }
    toOther = null;
    fromOther = null;
    return;
  }
}
