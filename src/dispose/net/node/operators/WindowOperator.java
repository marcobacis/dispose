package dispose.net.node.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.crypto.Data;

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

  protected int maxinputs = 1;
  
  protected int inputs = 0;
  
  /**
   * Represent the list of new elements added to each window,
   * but not processed yet by applyOpToWindow (useful for join)
   */
  protected List<List<DataAtom>> newElems;
  
  /**
   * Contains one window for each input stream (useful for joins or future operators)
   */
  protected List<Window> windows = new ArrayList<>();

  public WindowOperator(int id, int size, int slide, int inputs) {

    assert(size >= 1 && slide >= 1);

    this.id = id;
    this.clock = 0;
    this.size = size;
    this.slide = slide;
    
    this.inputs = inputs;
    this.newElems = new ArrayList<>(inputs);
    for(int i = 0; i < inputs; i++) {
      this.windows.add(new Window(size, slide));
      this.newElems.add(new LinkedList<>());
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
        this.windows.get(i).push(input[i]);
        this.newElems.get(i).add(input[i]);
        ready |= this.windows.get(i).ready();
      }
    }

    // apply operation on the windows

    if(ready){
      
      List<DataAtom> result = applyOpToWindows();
      
      //reset new elements for ready windows
      for(int w = 0; w < inputs; w++) {
        if(this.windows.get(w).ready()) {
          this.newElems.get(w).clear();
          this.windows.get(w).move();
        }
      }
      
      return result;
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
  public abstract Class<? extends DataAtom> outputRestriction(Class<? extends DataAtom> intype);

}
