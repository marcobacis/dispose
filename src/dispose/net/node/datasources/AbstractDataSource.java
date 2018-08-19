package dispose.net.node.datasources;

import java.util.concurrent.TimeUnit;

import dispose.net.common.Config;
import dispose.net.common.DataAtom;

abstract public class AbstractDataSource implements DataSource
{

  private static final long serialVersionUID = -7866911933157690908L;
  private int id;
  private int throttle = Config.minSourceThrottle;
  private int clock = 0;
  
  public AbstractDataSource(int id)
  {
    this.id = id;
  }
  
  public AbstractDataSource(int id, int throttle)
  {
    this(id);
    this.throttle = throttle;
  }
  
  @Override
  public int getID()
  {
    return id;
  }


  @Override
  public int clock()
  {
    return clock;
  }


  @Override
  public DataAtom nextAtom()
  {
    clock++;
    
    if (throttle > 0) {
      try {
        TimeUnit.MILLISECONDS.sleep(throttle);
      } catch (InterruptedException e) { /* who cares */ }
    }
    
    return getNextAtom();
  }
  
  protected abstract DataAtom getNextAtom();


  @Override
  abstract public void setUp();


  @Override
  abstract public void end();


  @Override
  abstract public Class<? extends DataAtom> outputRestriction();

}
