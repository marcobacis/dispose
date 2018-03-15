package dispose.net.node.operators;

import dispose.net.common.DataAtom;
import dispose.net.common.TypeSet;
import dispose.net.common.UniversalTypeSet;
import dispose.net.node.Operator;

public class NullOperator implements Operator
{
  int clock = 0;
  
  
  @Override
  public int clock()
  {
    return clock;
  }


  @Override
  public DataAtom processAtom(DataAtom input)
  {
    clock++;
    return input;
  }


  @Override
  public TypeSet inputRestriction()
  {
    return new UniversalTypeSet();
  }


  @Override
  public Class<DataAtom> outputRestriction(Class<DataAtom> intype)
  {
    return intype;
  }

}
