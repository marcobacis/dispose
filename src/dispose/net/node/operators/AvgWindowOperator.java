package dispose.net.node.operators;

import java.util.Collections;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class AvgWindowOperator extends WindowOperator
{
  
  private static final long serialVersionUID = 192613059444264258L;


  public AvgWindowOperator(int id, int size, int slide)
  {
    super(id, size, slide, 1);
  }


  @Override
  protected List<DataAtom> applyOpToWindows()
  {
    double sum = 0;
    for(DataAtom atom : this.windows.get(0).getElements()) {
      sum += ((FloatData) atom).floatValue();
    }
    
    return Collections.singletonList(new FloatData(sum / this.windows.get(0).size()));
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
