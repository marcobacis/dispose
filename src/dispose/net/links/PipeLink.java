package dispose.net.links;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import dispose.net.message.Message;

/**
 * Link implemented using a local pipe, for communication
 * between same-node threads
 */
public class PipeLink implements Link
{
  private PipedInputStream inPipe;
  private PipedOutputStream outPipe;
  
  private ObjectInputStream inStream;
  private ObjectOutputStream outStream;
  
  
  public PipeLink() throws IOException{
    this.outPipe = new PipedOutputStream();
    this.inPipe = new PipedInputStream();
    
    this.inPipe.connect(outPipe);
        
    this.outStream = new ObjectOutputStream(this.outPipe);
    this.inStream = new ObjectInputStream(this.inPipe);
  }
  
  
  @Override
  public InputStream getInputStream() throws IOException
  {
    return this.inStream;
  }


  @Override
  public OutputStream getOutputStream() throws IOException
  {
    return this.outStream;
  }

  
  @Override
  public void sendMsg(Message message) throws IOException
  {
    this.outStream.writeObject(message);
    this.outStream.flush();
  }


  @Override
  public Message recvMsg() throws IOException, ClassNotFoundException
  {
    return (Message) this.inStream.readObject();
  }
  
  
  @Override
  public Message recvMsg(int timeoutms) throws IOException, ClassNotFoundException
  {
    return recvMsg();
  }
  
  
  @Override
  public void close()
  {
    try{
      this.inStream.close();
      this.outStream.close();
      this.inPipe.close();
      this.outPipe.close();
    } catch(IOException e) {
      //does nothing
      return;
    }
  }
}
