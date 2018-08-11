package dispose.net.common.types;

import dispose.net.common.DataAtom;

public class FloatData extends DataAtom
{
  private double data;
  
  public FloatData(double value)
  {
    data = value;
  }
  
  public double floatValue()
  {
    return data;
  }
  
  @Override
  public String toString()
  {
    return "FloatData = " + Double.toString(data);
  }
}
