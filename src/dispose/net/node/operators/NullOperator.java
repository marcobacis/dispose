package dispose.net.node.operators;

import java.util.Collections;
import java.util.List;

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
  public List<DataAtom> processAtom(DataAtom... input)
  {
    clock++;
    return Collections.singletonList(input[0]);
  }


  @Override
  public TypeSet inputRestriction()
  {
    return new UniversalTypeSet();
  }


  @Override
  public Class<? extends DataAtom> outputRestriction(Class<? extends DataAtom> intype)
  {
    return intype;
  }


  @Override
  public int getID()
  {
    return 0;
  }

}
