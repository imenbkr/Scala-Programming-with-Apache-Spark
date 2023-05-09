package TD3_ex3

object TD3 {

  //1
  abstract class Forme {
    def surface: Double

    def calculSurface(): Double
  }

  //2
  trait Colorable {
    def affiche(): Unit = {
      println("colorable")
    }

    def colorer(couleur: String): Unit
  }


  //3
  class Rectangle(val longueur: Double, val largeur: Double) extends Forme with Colorable {
    var couleur: String = ""

    def calculSurface(): Double = {
      longueur * largeur
    }

    override def surface: Double = {
      calculSurface()
    }

    override def colorer(couleur: String): Unit = {
      this.couleur = couleur
    }
  }

  class Cercle(val rayon: Double) extends Forme with Colorable {
    var couleur: String = ""

    def calculSurface(): Double = {
      Math.PI * rayon * rayon
    }

    override def surface: Double = {
      calculSurface()
    }

    override def colorer(couleur: String): Unit = {
      this.couleur = couleur
    }
  }

  //4
  class Rectangle3D(val longueur: Double, val largeur: Double, val hauteur: Double) extends Forme {
    def calculSurface(): Double = {
      2 * (longueur * largeur + longueur * hauteur + largeur * hauteur)
    }

    override def surface: Double = {
      calculSurface()
    }
  }


}
