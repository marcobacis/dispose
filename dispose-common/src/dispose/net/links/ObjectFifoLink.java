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
  public void sendMsg(Message message) throws InterruptedException
  {
    toOther.put(message);
  }


  @Override
  public Message recvMsg() throws InterruptedException
  {
    return fromOther.take();
  }


  @Override
  public Message recvMsg(int timeoutms) throws InterruptedException
  {
    if (timeoutms == 0)
      return fromOther.take();
    return fromOther.poll(timeoutms, TimeUnit.MILLISECONDS);
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
