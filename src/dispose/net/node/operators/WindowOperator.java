package dispose.net.node.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.TypeSet;
import dispose.net.common.types.NullData;
import dispose.net.node.Operator;

public abstract class WindowOperator implements Operator, Serializable
{

  private static final long serialVersionUID = -7926914522640629497L;
  private int clock;
  private int size;
  private int slide;
  private int id; 

  protected int maxinputs = 0;
  
  protected int inputs = 0;
  protected List<Window> windows = new ArrayList<>();

  public WindowOperator(int id, int size, int slide, int inputs) {

    assert(size >= 1 && slide >= 1);

    this.id = id;
    this.clock = 0;
    this.size = size;
    this.slide = slide;
    
    this.inputs = inputs;
    for(int i = 0; i < inputs; i++) {
      this.windows.add(new Window(size, slide));
    }

  }

  @Override
  public int getID()
  {
    return this.id;
  }

  @Override
  public int clock()
  {
    return this.clock;
  }

  /**
   * Apply the Operator's operation to the current window
   * @return
   */
  protected abstract List<DataAtom> applyOpToWindows();

  @Override
  public List<DataAtom> processAtom(DataAtom... input)
  {
    this.clock++;

    assert(input.length == this.windows.size());

    boolean ready = false;

    //updates windows
    for(int i = 0; i < this.inputs; i++) {
      if(!(input[i] instanceof NullData)) {
        this.windows.get(i).push(input[0]);
        ready |= this.windows.get(i).ready();
      }
    }

    // apply operation on the windows

    if(ready){
      for(Window win : this.windows)
        win.move();
      
      return applyOpToWindows();
    }

    return new ArrayList<>();
  }

  /**
   * Reset all the parameters of the operators
   * clear the window without performing computations
   */
  public void reset() {
    this.clock = 0;
    for(Window win : this.windows)
      win.reset();
  }

  @Override
  public abstract TypeSet inputRestriction();


  @Override
  public abstract Class<DataAtom> outputRestriction(Class<DataAtom> intype);

}
