package dispose.net.node.operators;


import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;

public class IncrementOperator extends NullOperator
{

  @Override
  public DataAtom processAtom(DataAtom input)
  {
    FloatData fdata = (FloatData) input;
    return super.processAtom(new FloatData(fdata.floatValue()+1.0));
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
