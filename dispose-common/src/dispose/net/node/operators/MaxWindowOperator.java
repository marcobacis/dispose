package dispose.net.node.operators;


import java.util.Collections;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class MaxWindowOperator extends WindowOperator
{

  public MaxWindowOperator(int id, int size, int slide)
  {
    super(id, size, slide, 1);
  }


  @Override
  protected List<DataAtom> applyOpToWindows()
  {
    
    List<DataAtom> elements = this.windows.get(0).getElements();
    
    double max = ((FloatData) elements.get(0)).floatValue();
    for(int i = 1; i < elements.size(); i++) {
      double val = ((FloatData) elements.get(i)).floatValue();
      max = val > max ? val : max;
    }
    
    return Collections.singletonList(new FloatData(max));
  }


  @Override
  public TypeSet inputRestriction()
  {
    return new SingleTypeSet(FloatData.class);
  }


  @Override
  public Class<? extends DataAtom> outputRestriction(Class<? extends DataAtom> intype)
  {
    return intype;
  }

}
