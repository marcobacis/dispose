package dispose.net.links;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
  
  
  public PipeLink() throws IOException
  {
    this.outPipe = new PipedOutputStream();
    this.inPipe = new PipedInputStream();
  }
  
  
  public void connect(PipeLink oppositeLink) throws IOException
  {
    oppositeLink.inPipe.connect(this.outPipe);
    this.inPipe.connect(oppositeLink.outPipe);
    
    this.outStream = new ObjectOutputStream(this.outPipe);
    oppositeLink.outStream = new ObjectOutputStream(oppositeLink.outPipe);
    this.inStream = new ObjectInputStream(this.inPipe);
    oppositeLink.inStream = new ObjectInputStream(oppositeLink.inPipe);
  }

  
  @Override
  public void sendMsg(Message message) throws LinkBrokenException
  {
    try {
      this.outStream.flush();
      this.outStream.writeObject(message);
    } catch (IOException e) {
      throw new LinkBrokenException(e);
    }
  }


  @Override
  public Message recvMsg() throws LinkBrokenException
  {
    Message m;
    
    try {
      m = (Message)this.inStream.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new LinkBrokenException(e);
    }
    
    return m;
  }
  
  
  @Override
  public Message recvMsg(int timeoutms) throws LinkBrokenException
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
