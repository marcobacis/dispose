package dispose.net.node.threads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;
import dispose.net.links.Link;
import dispose.net.message.Message;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.datasources.DataSource;

public class SourceThread extends ComputeThread
{
  private DataSource dataSource;
  private List<Link> outStreams = new ArrayList<>();
  private AtomicBoolean running = new AtomicBoolean(false);
  private AtomicBoolean paused = new AtomicBoolean(false);
  
  private Object injectLock = new Object();
  
  public SourceThread(Node owner, DataSource dataSource)
  {
    super(owner);
    this.dataSource = dataSource;
    this.opID = dataSource.getID();
  }
  
  
  @Override
  public void setInputLink(Link inputLink, int fromId) throws Exception
  {
    throw new Exception("this is a data --> SOURCE <--");
  }


  @Override
  public void setOutputLink(Link outputLink, int toId) throws IOException
  {
    outStreams.add(outputLink);
  }


  @Override
  public void pause()
  {
    paused.set(true);
  }

  public void resume()
  {
    paused.set(false);
    synchronized(paused) {
      paused.notify();
    }
  }

  @Override
  public void start()
  {
    running.set(true);
    Thread thd = new Thread(() -> mainLoop());
    thd.setName("data-source-" + Integer.toString(getID()));
    thd.start();
  }
  
  @Override
  public void stop()
  {
    running.set(false);
    
    for(Link link : outStreams)
      link.close();
    
    dataSource.end();
  }
  
  private void mainLoop()
  {
    try {
      
      dataSource.setUp();
      
      while (true) {
        if(running.get() == false) return;
        
        if(paused.get()) {
          synchronized(paused) {
            while(paused.get()) {
                paused.wait();
            }
          }
        }
        
        synchronized (injectLock) {
          DataAtom d = dataSource.nextAtom();
          for (Link link: outStreams) {
            link.sendMsg(d);
          }
        }
      }
    } catch (Exception e) {
      DisposeLog.error(this, "oh oh we've lost the link \\OwO/");
      e.printStackTrace();
    }
  }
  
  public void injectMessage(Message toInject)
  {
    synchronized (injectLock) {
      for (Link link: outStreams) {
        link.sendMsg(toInject);
      }
    }
  }
  
}
