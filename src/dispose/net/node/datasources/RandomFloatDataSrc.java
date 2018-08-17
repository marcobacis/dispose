package dispose.net.node.datasources;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import dispose.net.common.DataAtom;
import dispose.net.common.types.FloatData;

public class RandomFloatDataSrc implements DataSource
{
  private static final long serialVersionUID = -3962038698442920907L;
  private int throttle;
  private int id;
  private int clock = 0;
  private transient Random rng;
  
  
  public RandomFloatDataSrc(int id, int throttlems)
  {
    this.id = id;
    this.throttle = throttlems;
  }
  
  
  @Override
  public int getID()
  {
    return id;
  }


  @Override
  public int clock()
  {
    return this.clock;
  }


  @Override
  public DataAtom nextAtom()
  {
    clock++;
    
    if (rng == null)
      rng = new Random();
    
    if (throttle > 0) {
      try {
        TimeUnit.MILLISECONDS.sleep(throttle);
      } catch (InterruptedException e) { /* who cares */ }
    }
    return new FloatData(7.0);//rng.nextDouble());
  }


  @Override
  public Class<? extends DataAtom> outputRestriction()
  {
    return FloatData.class;
  }


  @Override
  public void setUp()
  {
    //nothing to do here
  }


  @Override
  public void end()
  {
    //nothing to do here
  }
}
