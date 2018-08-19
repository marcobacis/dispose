package dispose.net.node.datasources;

import dispose.client.producer.SequenceProducerStream;
import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.common.types.FloatData;

public class SequenceDataSource extends AbstractDataSource
{

  private static final long serialVersionUID = 9034901424600021031L;
  private int end;
  private int count;
  
  public SequenceDataSource(SequenceProducerStream stream)
  {
    this(stream.getID(), stream.getThrottle(), stream.getStart(), stream.getEnd());
  }
  
  public SequenceDataSource(int id, int throttle, int start, int end)
  {
    super(id, throttle);
    this.count = start;
    this.end = end;
  }

  @Override
  public DataAtom getNextAtom()
  {
    if(end != 0 && count >= end)
      return new EndData();
    
    DataAtom newAtom = new FloatData(count);
    count++;
    return newAtom;
  }


  @Override
  public void setUp()
  {
    //do nothing
  }


  @Override
  public void end()
  {
    //do nothing
  }


  @Override
  public Class<? extends DataAtom> outputRestriction()
  {
    return FloatData.class;
  }

}
