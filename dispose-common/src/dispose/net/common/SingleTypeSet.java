package dispose.net.common;

public class SingleTypeSet extends TypeSet
{
  private Class<? extends DataAtom> content;
  
  
  public SingleTypeSet(Class<? extends DataAtom> type)
  {
    content = type;
  }
  
  
  @Override
  public boolean containsType(Class<? extends DataAtom> type)
  {
    return content == type;
  }

}
