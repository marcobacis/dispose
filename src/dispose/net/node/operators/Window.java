package dispose.net.node.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import dispose.net.common.DataAtom;
import dispose.net.common.types.NullData;


/**
 * Window class, it represents a count-based window
 * with parametrizable size and slide.
 *
 */
public class Window implements Serializable
{

  private static final long serialVersionUID = 4392413992419578170L;
  private int size;
  private int slide;

  protected ArrayList<DataAtom> window;
  private int currSlide = 0;

  private enum State {
    FILL, FULL;
  }

  private State state;

  public Window(int size, int slide) {

    assert(size >= 1 && slide >= 1);

    this.size = size;
    this.slide = slide;
    this.window = new ArrayList<DataAtom>();
    this.state = State.FILL;
  }

  /**
   * Returns the current window's elements
   * @return the current window's elements
   */
  public List<DataAtom> getElements() {
    List<DataAtom> elements = new ArrayList<>(this.window);
    return elements;
  }
  
  /**
   * Inserts an element into the window
   * @param input   The element to be pushed
   */
  public void push(DataAtom input)
  {

    if(!(input instanceof NullData)) {
      
      if(this.isFull()) this.window.remove(0);
      
      this.window.add(input);

      if(this.isFull()) this.currSlide++;

      //change state and set slide to the right one for the first window
      if(this.state == State.FILL && this.window.size() == this.size){
        this.state = State.FULL;
        this.currSlide = this.slide;
      }
    }

  }

  /**
   * Tests wether the window is full and has moved 
   * of the sliding factor.
   * @return true if ready for the operation, false otherwise
   */
  public boolean ready() {
    return this.isFull() && this.currSlide == this.slide;
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
    this.window.clear();
    this.state = State.FILL;
  }

  /**
   * Resets the current movement (slide) of the window
   */
  public void move() {
    this.currSlide = 0;
  }

  
  public int size() {
    return this.size;
  }
  
  public int slide() {
    return this.slide;
  }

}
