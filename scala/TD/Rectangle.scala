package TD

case class Rectangle (private var _x:Int=0, private var _y:Int=8){
  //we did a control of x, y => set x and y to private

  def x:Int = _x //getter
  def x_=(a :Int) = if(a>=0) _x=a else println("error, must be >=0!!") //setter

  def y: Int = _y //getter
  def y_=(a: Int) = if (a >= 0) _y = a else println("error, must be >=0!!") //setter
}


