object HelloWorld {
  def main(args : Array[String]): Unit = {
    val name: String = "Mark"
    val age: Int = 23
    println("Hello World")
    print {
      "my name is " + name + " and my age is " + age +" years old"
    }
    //println(s"$name is $age years old ")
    //println(f"$name%s is $age%d years old")
  }
}
