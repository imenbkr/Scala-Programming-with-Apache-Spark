import scala.Byte.MaxValue.%
import scala.math.Fractional.Implicits.infixFractionalOps
import scala.math.Integral.Implicits.infixIntegralOps
import scala.math.Numeric.BigDecimalAsIfIntegral.mkNumericOps
import scala.math._

object DS {
  def main(args: Array[String]): Unit = {
    //example1
    /*
    val result = power(2, 4)
    println(result)
     */
    //filtre1
    val Tab = Array[String]("Hello", "World", "append", "list")
    filtre(Tab, f).foreach(println)
    filtre(Tab, g) foreach println
    //filtre2
    val integers = Array[Int](1,2,-1,-2)
    //function(integers,  (x: Int) => x > 0) foreach(println)
    function(integers, (_: Int) > 0) foreach(println)

    //increase function



    //example2
    /*
    def estPalindrome(str: String): Boolean = {
      str == str.reverse
    }

    val arr = Array("radar", "scala", "kayak", "java", "level")
    val resultat = filtre(arr, estPalindrome)
    println(resultat.mkString(", ")) // Affiche "radar, kayak, level"

     */

    //example 3
    /*
    val arr = Array(-3, -2, -1, 0, 1, 2, 3)
    val result = filtre2(arr, (i: Int) => i > 0)
    println(result.mkString(", ")) // Affiche "1, 2, 3"

     */
    /*
    def sum(a: Int, b: Int, c: Int) = a + b + c

    sum(1, 2, 3)
    val a = sum  //fonction partielle
    print(a(1, 2, 3))

    val b = sum(1, _: Int, 3)
    print(b(2))
    */

    //exercice 6, td2
    def conditionelle[T](x: T, p : T => Boolean, f: T => T) =
      if (p(x)) f(x) else x

    /*val a = conditionelle(2, x => x%2 ==0 , x => x*x)
    val l = list(1,2,5,6,7)
     */

  }

  //function power
  def power(x: Double, n: Int): Double = {
    //math.pow(x, n)
    var R : Double = 1
    for (i<-1 to n) R= R*x
    return R
  }

  //function filtre
  val f =(x: String) => x.length%2==0
  val g =(x: String) => x.charAt(0)=='a'

  def filtre(arr: Array[String], f: String => Boolean): Array[String] = {
    arr.filter(f)
  }


  //function filtre (plus générale)
  def function[T](arr: Array[T], f: T => Boolean): Array[T] = {
    arr.filter(f)
  }


}
