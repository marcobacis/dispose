package dispose.net.node.datasinks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import dispose.net.common.DataAtom;
import dispose.net.common.types.FloatData;

public class FileDataSink implements DataSink
{

  private static final long serialVersionUID = 4436385634778545179L;

  private int id;
  private int clock = 0;
  
  private String path;
  private BufferedWriter outStream;
  

  public FileDataSink(int id, String path)
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
  public void processAtom(DataAtom atom)
  {
    try {
      outStream.write(((FloatData) atom).floatValue() + "\n");
    } catch (IOException e) {
      // do nothing..
    }
    clock++;
  }


  @Override
  public boolean inputRestriction(Class<? extends DataAtom> input)
  {
    return input == FloatData.class;
  }

  @Override
  public void end()
  {
    try {
      this.outStream.close();
    } catch (IOException e) {
      // nothing to do
    }
  }
  
  @Override
  public void setUp()
  {
    try {
      this.outStream = new BufferedWriter(new FileWriter(new File(this.path)));
    } catch (IOException e) {
      //TODO send back error to supervisor
    }
  }

}
