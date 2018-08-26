
package dispose.net.node.datasources;

import java.io.*;

import dispose.client.producer.FileProducerStream;
import dispose.net.common.DataAtom;
import dispose.net.common.types.EndData;
import dispose.net.common.types.FloatData;


public class FileDataSource extends AbstractDataSource
{

  private static final long serialVersionUID = -7965858251778817607L;

  private String path;
  private BufferedReader inStream;

  private int lastLine = 0;


  public FileDataSource(FileProducerStream stream)
  {
    this(stream.getID(), stream.getFilePath());
  }


  public FileDataSource(int id, String path)
  {
    super(id);
    this.path = path;
  }


  @Override
  public DataAtom getNextAtom()
  {
    String line;

    try {
      line = inStream.readLine();

      if (line != null)
        return new FloatData(this.clock(), Double.parseDouble(line));

      this.end();
      return new EndData(this.clock());

    } catch (IOException e) {
      // TODO send error to supervisor or just end computation?
      return new EndData(this.clock());
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
  public void restart()
  {
    end();
    setUp();

    // reads all the lines till the last one
    for (int l = 0; l < lastLine; l++) {
      try {
        inStream.readLine();
      } catch (IOException e) {
        //TODO as the others...send message to supervisor?
      }
    }
  }


  @Override
  public Class<? extends DataAtom> outputRestriction()
  {
    return FloatData.class;
  }

}
