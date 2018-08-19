package dispose.net.links;

import dispose.net.message.Message;

/** Interface for a generic duplex communication link used by Dispose. Not thread-safe. */
public interface Link
{ 
  public void sendMsg(Message message) throws LinkBrokenException;
  
  public Message recvMsg() throws LinkBrokenException;
  
  /** Receives a message from the opposite side of the link.
   * @param timeoutms The time of the timeout in milliseconds or zero for waiting forever
   * @return The received message or null in case the timeout expired
   * @throws LinkBrokenException If the link broke during the wait */
  public Message recvMsg(int timeoutms) throws LinkBrokenException;
  
  /** Closes the link. After the link is closed, all send or receive methods will immediately
   * throw LinkBrokenException. */
  public void close();
}
