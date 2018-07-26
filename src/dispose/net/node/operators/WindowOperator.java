package dispose.net.node.operators;

import java.util.ArrayList;

import dispose.net.common.DataAtom;
import dispose.net.common.TypeSet;
import dispose.net.common.types.NullData;
import dispose.net.node.Operator;

public abstract class WindowOperator implements Operator
{
  
  private int clock;
  private int size;
  private int slide;
  
  protected ArrayList<DataAtom> window;
  private int currSlide = 0;
  
  private enum State {
    FILL, FULL;
  }
  
  private State state;
  
  public WindowOperator(int size, int slide) {
    this.clock = 0;
    this.size = size;
    this.slide = slide;
    this.window = new ArrayList<DataAtom>();
    this.state = State.FILL;
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
  protected abstract DataAtom applyOpToWindow();

  @Override
  public DataAtom processAtom(DataAtom input)
  {
    this.clock++;
    
    //updates window
    
    if(this.isFull()) this.window.remove(this.window.size()-1);
    
    if(!(input instanceof NullData)) {
      this.window.add(0,input);
      
      if(this.isFull()) this.currSlide++;
      
      //change state and set slide to the right one for the first window
      if(this.state == State.FILL && this.window.size() == this.size){
        this.state = State.FULL;
        this.currSlide = this.slide;
      }
    }
        
    // apply operation on the window
    
    if(this.isFull() && this.currSlide == this.slide){
      this.currSlide = 0;
      return applyOpToWindow();
    }
    
    return new NullData();
  }

  /**
   * Returns whether the window is full or not
   * @return true if the window is full, false otherwise
   */
  public boolean isFull() {
    return this.state == State.FULL;
  }
  
  /**
   * Returns whether the window is empty or not
   * @return true if the window is empty, false otherwise
   */
  public boolean isEmpty() {
    return this.window.size() == 0;
  }
  
  /**
   * Reset all the parameters of the operators
   * clear the window without performing computations
   */
  public void reset() {
    this.currSlide = 0;
    this.clock = 0;
    this.window.clear();
    this.state = State.FILL;
  }

  @Override
  public abstract TypeSet inputRestriction();


  @Override
  public abstract Class<DataAtom> outputRestriction(Class<DataAtom> intype);

}
