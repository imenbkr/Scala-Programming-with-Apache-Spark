package TD

import TD.Voiture.vitesseMax

class Voiture private (val fabricant: String, val modele: String) {
  //private : to access the method apply and not do new constructor
  var vitesse: Double = 0;
  var isOn: Boolean = false;

  def start(): Unit = {
    isOn = true
    println(s"Voiture started ")
  }

  def accelerer(rate: Double): Unit = {
    vitesse += rate
    if(vitesse > Voiture.vitesseMax){
      println(s"on a atteint la vitesse maximale : ${Voiture.vitesseMax}")
      vitesse= Voiture.vitesseMax;
    }
    else {
      println(s"Voiture accelere a $rate par seconde.")
    }
  }

  def decelerer(rate: Double): Unit = {
    vitesse -= rate
    println(s"Voiture ralenti a $rate par seconde.")
  }

  def stop(): Unit = {
    vitesse = 0;
    isOn = false;
    println("Voiture s’arrete.")
  }

}
object Voiture{
  val vitesseMax=100;
  def apply(fabricant: String, modele: String) = new Voiture(fabricant, modele);
}

  object TestVoiture { // extends App
    def main(args: Array[String]): Unit = {
      //val v = new Voiture("Toyota", "Rav4");
      //declare apply method
      val v = Voiture.apply("Toyota", "Rav4");
      v.start();
      v.accelerer(110);
      v.decelerer(10);
      v.stop();

    }
  }
