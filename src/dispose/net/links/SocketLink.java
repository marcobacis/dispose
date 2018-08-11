package dispose.net.links;

import java.io.*;
import java.net.*;

import dispose.net.message.Message;


/**
 * Link implemented using sockets and object streams,
 * to be used on different nodes (processes/machines).
 */
public class SocketLink implements Link
{
  Socket sock;
  
  ObjectInputStream inStream;
  ObjectOutputStream outStream;
  
  /**
   * Constructor. Takes the socket and derives the streams
   * @param sock    Socket to use
   * @throws IOException
   */
  private SocketLink(Socket sock) throws IOException {
    this.sock = sock;
    this.outStream = new ObjectOutputStream(sock.getOutputStream());
    this.inStream = new ObjectInputStream(sock.getInputStream());
  }
  
  /**
   * Connects to the given host+port and returns the newly created link
   * @param host    Host to connect to
   * @param port    Port to connect to
   * @return        The Link created from the connection
   * @throws UnknownHostException
   * @throws IOException
   */
  public static SocketLink connectTo(String host, int port) throws UnknownHostException, IOException {

    Socket sock = new Socket(host, port);

    System.out.println("Connected at " + host + " port " + port);
    
    return new SocketLink(sock);
  }
  
  /**
   * Accepts any new connections on the given port and returns the corresponding link
   * @param port    Port on which to accept connections
   * @return        The Link created from the connection
   * @throws IOException
   */
  public static SocketLink connectFrom(int port) throws IOException {
    
    System.out.println("Waiting on port " + port);
    ServerSocket server = new ServerSocket(port);
    
    Socket sock = server.accept();
    
    System.out.println("Accepted on port " + port);
    
    server.close();
    
    return new SocketLink(sock);
  }
  
  
  @Override
  public void sendMsg(Message message) throws IOException
  {
    this.outStream.writeObject(message);
    this.outStream.flush();
  }
  
  
  @Override
  public Message recvMsg(int timeoutms) throws IOException, ClassNotFoundException
  {
    Message res;
    this.sock.setSoTimeout(timeoutms);
    try {
      res = (Message)this.inStream.readObject();
    } catch (SocketTimeoutException e) {
      res = null;
    }
    return res;
  }


  @Override
  public Message recvMsg() throws IOException, ClassNotFoundException
  {
    return recvMsg(0);
  }

  
  @Override
  public void close()
  {
    try{
      this.inStream.close();
      this.outStream.close();
      this.sock.close();
    } catch(IOException e) {
      //does nothing
      return;
    }
  }
  
  
  public String remoteHostAddress()
  {
    return sock.getInetAddress().getHostAddress();
  }
}
