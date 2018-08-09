package dispose.net.links;

import java.io.*;

import dispose.net.message.Message;

public interface Link
{
  
  /**
   * @return the link's input stream
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException;
  
  /**
   * @return the link's output stream
   * @throws IOException
   */
  public OutputStream getOutputStream() throws IOException;
  
  
  public void sendMsg(Message message) throws IOException;
  
  public Message recvMsg() throws IOException, ClassNotFoundException;
  
  public Message recvMsg(int timeoutms) throws IOException, ClassNotFoundException;
  
  /**
   * Closes the link
   */
  public void close();
}
