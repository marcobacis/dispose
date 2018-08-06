package dispose.net.links;

import java.io.*;

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
  
  
  public void sendMsg(Object message) throws IOException;
  
  public Object recvMsg() throws IOException, ClassNotFoundException;
  
  public Object recvMsg(int timeoutms) throws IOException, ClassNotFoundException;
  
  /**
   * Closes the link
   */
  public void close();
}
