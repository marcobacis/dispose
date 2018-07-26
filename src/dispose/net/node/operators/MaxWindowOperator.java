package dispose.net.node.operators;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class MaxWindowOperator extends WindowOperator
{

  public MaxWindowOperator(int size, int slide)
  {
    super(size, slide);
  }


  @Override
  protected DataAtom applyOpToWindow()
  {
    
    double max = ((FloatData) this.window.get(0)).floatValue();
    for(int i = 1; i < this.window.size(); i++) {
      double val = ((FloatData) this.window.get(i)).floatValue();
      max = val > max ? val : max;
    }
    
    return new FloatData(max);
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
