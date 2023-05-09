import scala.io.StdIn.readInt
import scala.language.postfixOps

object Test1 {
  def main(args: Array[String]): Unit = {
    //println("Returned Value : " + addInt(3, 2));
    //println("pgcd = " + pgcd(7, 3))
    //println(nbrPremiers(21))
    //print(average())
  }

  def addInt(a: Int, b: Int): Int = {
    val sum : Int = if(a==b)  a*3
                    else  a+b
    sum
  }

  /*
  val nom: String  = readLine("donnez votre nom: ")
  val age : Int = readInt()
  val moyenne : Float = readFloat()
  /*print(f""" nom = $nom
                 agé(e) = $age
                 a pour moyenne = $moyenne%2f """)
   */
  println(f"l'étudiant(e) $nom%s agé(e) $age%d a eu la moyenne $moyenne%2f")
  println(s"l'étudiant(e) =$nom agé(e) = $age a eu la moyenne = $moyenne ")
  println("l'étudiant(e) "+nom+"\n agé(e) "+age +"\n a eu la moyenne "+moyenne)
  printf("l'étudiant(e) %s agé(e) %d a eu la moyenne %f", nom, age, moyenne)
   */

  /*def pgcd(a: Int, b: Int): Int = {
    val r: Int = a % b
    if (r == 0) return b
    return pgcd(b, r)
  }
  */
  def pgcd(a: Int, b: Int): Int = {
    val min= if (a > b) b else a
    val div = for(i<-1 to min if(a % i == 0 && b % i == 0)) yield i
    div.max
  }

  println("saisir un nombre n:")
  val n = readInt()
  val premier = for (i <- 2 to n if (2 until i).forall(j => i % j != 0)) yield i
  //for(i<-2 to n if(for(j<-2 to i/2 if (i % j != 0) )) yield j isEmpty ) yield i
  print(premier)

  def nbrPremiers(max: Int) {
    var i = 2
    while ((i <= max) && (max > 20)) {
      var premier = 1
      for (loop <- 2 to i) {
        if ((i % loop) == 0 && loop != i) premier = 0
      }
      if (premier != 0) {
        println(i)
        i += 1
      }
      else i += 1
    }
  }

  def average(): Float = {

      var k : Float =0
      var sum : Float = 0

      print("Enter a positive integer or -1 to quit: ")
      var num = scala.io.StdIn.readInt()
      while(num != -1)
      { if (num<0) {
        println("Enter a positive number ")
        num = scala.io.StdIn.readInt()
      }
        if (num>0)
        {sum = sum + num
        k += 1
        print("Enter a positive integer or -1 to quit: ")
        num = scala.io.StdIn.readInt()
        }

      }
     val moy : Float = sum / k
     return moy

  }

  def tableau (): Unit = {
    //user input
    println("Enter the size of the array (0 to 10): ")
    val n = scala.io.StdIn.readInt()

    if (n == 0) {
      println("Array is empty.")
    } else{
      val myList = new Array[Int](n)
      println("the values of the array :")
      for(i<-0 until n){
        println(s"Enter number $i: ")
        myList(i) = scala.io.StdIn.readInt()
    }

    // Print all the array elements
    for (x <- myList) {
      println(x)
    }
    //myList.foreach(println)

    //Sum of all elements
    var sum= 0.0;
    for(i<-0 to (myList.length-1)) {
      sum+=myList(i)
    }
    print("sum of element: "+sum)

    //finding the largest element
    var max = 0
    for (i <- 0 to (myList.length-1))
      {
        if(myList(i)>max) max=myList(i)
      }
    println("max is : "+max)

    //finding the smallest element
    var min = 0
    for (i <- 0 to (myList.length-1)){
      if(myList(i)<min) min=myList(i)
    }
    print("min is :"+ min)
  }
    //sort the array in ascending order
    /*
    for (i <- 0 until n - 1) {
      for (j <- 0 until n - i - 1) {
        if (myList(j) > myList(j + 1)) {
          val temp = myList(j)
          myList(j) = myList(j + 1)
          myList(j + 1) = temp

        }
      }
    }
    */
}
}