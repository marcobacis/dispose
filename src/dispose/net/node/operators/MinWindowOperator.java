package dispose.net.node.operators;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class MinWindowOperator extends WindowOperator
{

  public MinWindowOperator(int size, int slide)
  {
    super(size, slide);
  }


  @Override
  protected DataAtom applyOpToWindow()
  {
    double min = ((FloatData) window.get(0)).floatValue();
     
    for(int i = 1; i < this.window.size(); i++) {
      double val = ((FloatData) window.get(i)).floatValue();
      min = val < min ? val : min;
    }
    
    return new FloatData(min);
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
