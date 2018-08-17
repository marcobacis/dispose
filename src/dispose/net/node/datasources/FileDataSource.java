package dispose.net.node.datasources;

import java.io.*;

import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.common.types.FloatData;

public class FileDataSource implements DataSource
{

  private static final long serialVersionUID = -7965858251778817607L;
  private int id;
  private int clock = 0;
  
  private String path;
  private BufferedReader inStream;
  
  public FileDataSource(int id, String path)
  {
    this.id = id;
    this.path = path;
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
  public DataAtom nextAtom()
  {
    String line;
    try {
      line = inStream.readLine();
    
    
    if(line != null)
      return new FloatData(Double.parseDouble(line));
    
    this.end();
    return new EndData();
    
    } catch (IOException e) {
      //TODO send error to supervisor or just end computation?
      return new EndData();
    }
  }


  @Override
  public void setUp()
  {
    try {
      inStream = new BufferedReader(new FileReader(path));
    } catch (FileNotFoundException e) {
      // TODO send error to supervisor and block everything
    }
  }


  @Override
  public void end()
  {
    try {
      inStream.close();
    } catch (IOException e) {
      // do nothing
    }
  }


  @Override
  public Class<? extends DataAtom> outputRestriction()
  {
    return FloatData.class;
  }

}
