
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
    if (throttle > 0) {
      try {
        TimeUnit.MILLISECONDS.sleep(throttle);
      } catch (InterruptedException e) {
        /* who cares */ }
    }

    DataAtom next = getNextAtom();
    clock++;

    return next;
  }


  protected abstract DataAtom getNextAtom();


  @Override
  public void setUp()
  {
    // Nothing to do
  }


  @Override
  public void end()
  {
    // Nothing to do
  }


  @Override
  public void restart()
  {
    end();
    setUp();
  }


  @Override
  abstract public Class<? extends DataAtom> outputRestriction();

}
