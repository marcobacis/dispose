package dispose.net.links;

import java.io.*;

import dispose.net.message.Message;

public interface Link
{ 
  
  public void sendMsg(Message message) throws Exception;
  
  public Message recvMsg() throws Exception;
  
  public Message recvMsg(int timeoutms) throws Exception;
  
  /**
   * Closes the link
   */
  public void close();
}
