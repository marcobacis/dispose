
package dispose.net.node.checkpoint;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import dispose.net.message.Message;
import dispose.net.node.datasources.DataSource;


public class DataSourceCheckpoint extends Checkpoint
{

  private static final long serialVersionUID = -3612779999040226454L;
  private LinkedBlockingQueue<Message> injectQueue;


  public DataSourceCheckpoint(UUID chkpid, DataSource source,
    LinkedBlockingQueue<Message> injectQueue)
  {
    super(chkpid, source);
    this.injectQueue = new LinkedBlockingQueue<>(injectQueue);
  }


  public LinkedBlockingQueue<Message> getInjectQueue()
  {
    return injectQueue;
  }
}
