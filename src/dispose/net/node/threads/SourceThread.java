package dispose.net.node.threads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;
import dispose.net.links.Link;
import dispose.net.node.ComputeThread;
import dispose.net.node.datasources.DataSource;

public class SourceThread extends ComputeThread
{
  private DataSource dataSource;
  private List<Link> outStreams = new ArrayList<>();
  private AtomicBoolean running = new AtomicBoolean(true);
  
  
  public SourceThread(DataSource dataSource)
  {
    this.dataSource = dataSource;
    this.opID = dataSource.getID();
  }
  
  
  @Override
  public void addInput(Link inputLink) throws Exception
  {
    throw new Exception("this is a data --> SOURCE <--");
  }


  @Override
  public void addOutput(Link outputLink) throws IOException
  {
    outStreams.add(outputLink);
  }


  @Override
  public void pause()
  {
    running.set(false);
  }


  @Override
  public void start()
  {
    Thread thd = new Thread(() -> mainLoop());
    thd.setName("data-source-" + Integer.toString(getID()));
    thd.start();
  }
  
  
  private void mainLoop()
  {
    try {
      while (running.get()) {
        DataAtom d = dataSource.nextAtom();
        for (Link link: outStreams) {
          link.sendMsg(d);
        }
      }
    } catch (Exception e) {
      DisposeLog.error(this, "oh oh we've lost the link \\OwO/");
      e.printStackTrace();
    }
  }
}
