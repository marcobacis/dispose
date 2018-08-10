package dispose.net.links;

import java.io.*;

import dispose.net.message.Message;

public interface Link
{ 
  
  public void sendMsg(Message message) throws IOException;
  
  public Message recvMsg() throws IOException, ClassNotFoundException;
  
  public Message recvMsg(int timeoutms) throws IOException, ClassNotFoundException;
  
  /**
   * Closes the link
   */
  public void close();
}
