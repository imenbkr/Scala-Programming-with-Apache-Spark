package TD

object Test extends App{
  val R1 = Rectangle(2, 8)
  println(R1) // Rectangle(2,8)

  R1.x = -4 // largeur doit etre >= 0 !!!
  //R1.x : x is a function not an attribute
  R1.y = -7 // longueur doit etre >= 0 !!!
  println(R1) // Rectangle(2,8)

  val R2 = Rectangle(2)
  println(R2) // Rectangle(2,8)

  if(R1 == R2) println("equals") else println("not equals") // equals

  val R3 = R2.copy()
  println(R3) // Rectangle(5,8)

  if(R3 == R2) println("equals") else println("not equals") // equals

  R3.x=3
  println(R2) // Rectangle(5,8)
  println(R3) // Rectangle(3,8)
}

