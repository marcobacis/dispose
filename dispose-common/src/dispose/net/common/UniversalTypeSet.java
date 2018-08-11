package dispose.net.common;

public class UniversalTypeSet extends TypeSet
{

  @Override
  public boolean containsType(Class<? extends DataAtom> type)
  {
    return true;
  }

}
