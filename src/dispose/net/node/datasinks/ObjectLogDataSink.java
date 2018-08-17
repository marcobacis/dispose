package dispose.net.node.datasinks;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;

public class ObjectLogDataSink implements DataSink
{
  private static final long serialVersionUID = 8823023649552312928L;
  private int id;
  private int clock = 0;
  
  
  public ObjectLogDataSink(int id)
  {
    this.id = id;
  }
  
  @Override
  public int getID()
  {
    return id;
  }


  @Override
  public int clock()
  {
    return clock;
  }


  @Override
  public void processAtom(DataAtom atom)
  {
    DisposeLog.debug(this, "[objid=", id, "] ", atom.toString());
    clock++;
  }


  @Override
  public boolean inputRestriction(Class<? extends DataAtom> input)
  {
    return true;
  }

  @Override
  public void end()
  {
    // nothing to do here
  }

  @Override
  public void setUp()
  {
    //nothing to do here
  }
  
}
