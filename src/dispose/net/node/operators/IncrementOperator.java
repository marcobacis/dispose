package dispose.net.node.operators;


import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class IncrementOperator extends NullOperator
{

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
  public Class<DataAtom> outputRestriction(Class<DataAtom> intype)
  {
    return intype;
  }

}
