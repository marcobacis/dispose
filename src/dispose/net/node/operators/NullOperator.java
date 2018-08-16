package dispose.net.node.operators;

import java.util.Collections;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.TypeSet;
import dispose.net.common.UniversalTypeSet;

public class NullOperator implements Operator
{
  private static final long serialVersionUID = 1L;
  private int clock = 0;
  private int id;
    
  public NullOperator(int id)
  {
    this.id = id;
  }
  
  
  @Override
  public int clock()
  {
    return clock;
  }

  @Override
  public int getNumInputs()
  {
    return 1;
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
    return id;
  }

}
