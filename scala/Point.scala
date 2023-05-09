/*we can also write: class Point_(var x:Int =0, var y:Int =0)
val p2 = new Point_(y=1) or : val p3 = new Point_(1) returns x=1, y=0
in the case : class Point_(var x:Int, var y:Int) the default constructor is not valid (we have 2 parameters)
in the case we write default values for each parameter we can write val ... new Point_(6)
 */
class Point_(var x:Int =0, var y:Int =0){
  /*
  if we write class Point ...
  var x: Int = 0 //default = public
  var y: Int = 0


  //auxiliary constructor
  def this(a: Int, b: Int) = {
    //call of primary constructor
    this()
    x = a
    y = b
  }
  def this(a:Int) = {
    this()
    x= a
    y=a
  }

   */

  //methode affiche
  def affiche() = println(s"x = $x, y= $y")
}

//objet companion
object Point1{ //no constructor
  private val limit :Int =0
  def affiche(p:Point1)= println(p._x)

  def apply(a:Int, b:Int) : Point1 = new Point1(a, b)

  def apply(a:Int) : Point1 = new Point1(a)

  def apply() : Point1 = new Point1()

  //def apply(): Unit = println("hello") in test => Point1() =>"hello"

  //def unapply()

}
class Point1(private var _x:Int =0, private var _y:Int =0){
  def x=_x
  def x_=(a:Int)= if(a > Point1.limit) _x=a else println("error")
}

//


class Point3D(var x:Int, var y:Int){
  var z:Int =0
}


class Point2(x:Int, y:Int){
  //default : x,y : private val
  def affiche() = println(s"x = $x, y= $y")
  //x and y can be actual parameters, can be accessible inside class : private (not local parameters)
  //def setX(a:Int) = x=a =>reassignment to val
}


class Complexe(var a:Int=0, var b:Int=0){
  //def afficher() = println(s"real = $a, img= $b") =>affiche des adresses
  override def toString: String = s"a =$a, b =$b"
  def +(c:Complexe) = new Complexe(a +c.a, b+ c.b)

  def +(c:Int) : Complexe = this+ new Complexe(c)
}

  object Test { // extends App
    def main(args: Array[String]): Unit = {
      val p = new Point_(2,3) //instance de la classe point
      //we can also write new Point
      //in java we can't use constructor without parameters if we have another one with parameters

      val p1 = new Point_(6)
      val p2 = new Point_(y=4)
      val p3 = new Point2(10,11)
      //cant do p3.x => x is private
      //p3.affiche()
      /*
      p.affiche()
      p1.affiche()
      p2.affiche()
       */


      /*
      val c1 = new Complexe(1,2)
      val c2 = new Complexe(3)
      val c3 = c1.+(c2)
      val c4 = c1 +c2
      println(c3)
      println(c4)


      val c5 = c2 + 9
      println(c5)

       */


      val point= new Point1()
      point.x = 4
      // point.y = 5 doesnt work
      //print(point.x)
      //point._x doesnt work
      //Point1.affiche(point)
      //println(Point1.limit) error => limit is private in object can't be accessible

      var point2  = Point1.apply(4,9)
      var point3  = Point1(5,8) //point3 = Point1.apply(5,8)
      var point4  = Point1(7)
      var point5  = Point1()

      val l = List(2,3,5) //appel à la méthode apply de la classe List

    }
}
