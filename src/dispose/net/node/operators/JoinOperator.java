
package dispose.net.node.operators;

import java.util.LinkedList;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.SingleTypeSet;
import dispose.net.common.TypeSet;
import dispose.net.common.types.FloatData;
import dispose.net.common.types.NullData;


public class JoinOperator extends WindowOperator
{
  private static final long serialVersionUID = -6180543417088480518L;


  public JoinOperator(int id, int size, int slide, int inputs)
  {
    super(id, size, slide, inputs);
  }


  @Override
  protected List<DataAtom> applyOpToWindows()
  {
    List<DataAtom> left = this.windows.get(0).getElements();
    List<DataAtom> right = this.windows.get(1).getElements();
    
    int newLeft = this.newElems.get(0);
    int newRight = this.newElems.get(1);
    
    List<DataAtom> result = new LinkedList<>();

    //TODO extend to multiple concurrent join (> 2 streams, for now just 2 at a time as in the client API) 
    
    //joins forward (new elems on left -> all elements on right)
    int leftNewIdx = Math.max(left.size() - newLeft, 0);
    List<DataAtom> toJoin = left.subList(leftNewIdx, left.size());
    List<DataAtom> toBeJoined = right;
    result.addAll(join(toJoin, toBeJoined));
    
    //joins backward (old elements on left -> new elements on right)
    int rightNewIdx = Math.max(right.size() - newRight, 0);
    toJoin = left.subList(0, leftNewIdx);
    toBeJoined = right.subList(rightNewIdx, right.size());
    result.addAll(join(toJoin, toBeJoined));
        
    return result;
  }

  protected List<DataAtom> join(List<DataAtom> left, List<DataAtom> right) {
    
    List<DataAtom> result = new LinkedList<>();
    
    for(DataAtom leftAtom : left) {
      for(DataAtom rightAtom : right) {
        DataAtom joinRes = join(leftAtom, rightAtom);
        if(joinRes != null && !(joinRes instanceof NullData))
          result.add(joinRes);
      }
    }
       
    return result;
  }

  protected DataAtom join(DataAtom left, DataAtom right)
  {

    if (left.getClass() != right.getClass())
      return new NullData();

    if (left instanceof FloatData) {
      if (((FloatData) left).floatValue() == ((FloatData) right).floatValue())
        return left;
    }

    return new NullData();
  }


  @Override
  public TypeSet inputRestriction()
  {
    return new SingleTypeSet(FloatData.class);
  }


  @Override
  public Class<? extends DataAtom>
  outputRestriction(Class<? extends DataAtom> intype)
  {
    return intype;
  }

}
