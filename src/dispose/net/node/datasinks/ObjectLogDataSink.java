package dispose.net.node.datasinks;

import java.util.HashMap;
import java.util.Map;

import dispose.log.DisposeLog;
import dispose.net.common.DataAtom;

public class ObjectLogDataSink implements DataSink
{
  private static final long serialVersionUID = 8823023649552312928L;
  private int id;
  private Map<Integer, Long> streamClocks = new HashMap<>();
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
  public void processAtom(DataAtom atom, int sourceId)
  {
    Long lastTs = streamClocks.get(sourceId);
    if (lastTs == null)
      lastTs = (long) -1;
    if (lastTs < atom.getTimestamp()) {
      streamClocks.put(sourceId, atom.getTimestamp());
      DisposeLog.debug(this, "[objid=", id, "] ", atom.toString());
    }
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
