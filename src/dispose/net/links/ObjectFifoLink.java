package dispose.net.links;

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
    
    return msg;
  }


  @Override
  public void close()
  {
    // TODO kill paired link on close
    toOther = null;
    fromOther = null;
    return;
  }
}
