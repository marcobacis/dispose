package dispose.net.node.operators;


import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class IncrementOperator extends NullOperator
{
  private static final long serialVersionUID = -296933436361640146L;


  public IncrementOperator(int id)
  {
    super(id);
  }


  @Override
  public List<DataAtom> processAtom(DataAtom input[])
  {
    FloatData fdata = (FloatData) input[0];
    
    DataAtom[] outData = { new FloatData(fdata.floatValue() + 1.0) };
    
    return super.processAtom(outData);
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
