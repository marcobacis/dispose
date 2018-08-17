package dispose.net.links;

import dispose.net.message.Message;

public interface Link
{ 
  
  public void sendMsg(Message message) throws LinkBrokenException;
  
  public Message recvMsg() throws LinkBrokenException;
  
  /** Receives a message from the opposite side of the link.
   * @param timeoutms The time of the timeout in milliseconds or zero for waiting forever
   * @return The received message or null in case the timeout expired
   * @throws LinkBrokenException If the link broke during the wait */
  public Message recvMsg(int timeoutms) throws LinkBrokenException;
  
  /**
   * Closes the link
   */
  public void close();
}
