
package dispose.net.node.datasources;

import java.util.Random;

import dispose.net.common.DataAtom;
import dispose.net.common.types.FloatData;


public class RandomFloatDataSrc extends AbstractDataSource
{
  private static final long serialVersionUID = -3962038698442920907L;
  private transient Random rng;


  public RandomFloatDataSrc(int id, int throttlems)
  {
    super(id, throttlems);
  }


  @Override
  public DataAtom getNextAtom()
  {
    if (rng == null)
      rng = new Random();

    return new FloatData(this.clock(), rng.nextInt(2));
  }


  @Override
  public Class<? extends DataAtom> outputRestriction()
  {
    return FloatData.class;
  }

}
