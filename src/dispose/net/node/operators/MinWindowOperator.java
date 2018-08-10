package dispose.net.node.operators;

import java.util.Collections;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class MinWindowOperator extends WindowOperator
{

  private static final long serialVersionUID = -5565985256747810409L;


  public MinWindowOperator(int id, int size, int slide)
  {
    super(id, size, slide, 1);
  }


  @Override
  protected List<DataAtom> applyOpToWindows()
  {
    
    List<DataAtom> elements = this.windows.get(0).getElements();
    
    double min = ((FloatData) elements.get(0)).floatValue();
     
    for(int i = 1; i < elements.size(); i++) {
      double val = ((FloatData) elements.get(i)).floatValue();
      min = val < min ? val : min;
    }
    
    return Collections.singletonList(new FloatData(min));
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
