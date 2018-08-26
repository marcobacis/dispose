
package dispose.net.common.types;

import dispose.net.common.DataAtom;


public class FloatData extends DataAtom
{
  private static final long serialVersionUID = 3328508760950595947L;
  private double data;


  public FloatData(long timestamp, double value)
  {
    super(timestamp);
    data = value;
  }


  public double floatValue()
  {
    return data;
  }


  @Override
  public boolean equals(Object o)
  {

    if (o == this)
      return true;

    if (!(o instanceof FloatData))
      return false;

    double val = ((FloatData) o).floatValue();

    return Double.compare(floatValue(), val) == 0;
  }


  @Override
  public String toString()
  {
    return "FloatData = " + Double.toString(data);
  }
}
