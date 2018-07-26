package dispose.net.node.operators;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class AvgWindowOperator extends WindowOperator
{

  public AvgWindowOperator(int size, int slide)
  {
    super(size, slide);
  }


  @Override
  protected DataAtom applyOpToWindow()
  {
    double sum = 0;
    for(DataAtom atom : this.window) {
      sum += ((FloatData) atom).floatValue();
    }
    
    return new FloatData(sum / this.window.size());
  }


  @Override
  public TypeSet inputRestriction()
  {
    return new SingleTypeSet(FloatData.class);
  }


  @Override
  public Class<DataAtom> outputRestriction(Class<DataAtom> intype)
  {
    return intype;
  }

}
