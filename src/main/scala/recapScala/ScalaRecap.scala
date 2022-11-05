package recapScala

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if (2 < 3) "Big" else "Small"

  // instructions vs expressions
  val theUnit = println("Hello,Scala")

  // functions
  def myFunctions(x: Int) = 43

  //OOP
  class Animal

  class Dog extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Eating")
  }

  // singleton pattern
  object MySingleton

  // Companions
  object Carnivore

  // generics
  trait MyList[+A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)
  println(y)

  // functional Programming
  val incrmenter: Int => Int = x => x + 1
  val incremented = incrmenter(42)

  // map , flatMap, filter
  val processedList = List(1,2,3).map(incrmenter)

  // Pattern Matching
  val unknown: Any = 45
  val case1 = unknown match {
    case 1 => "f"
    case 45 => "a"
    case _ => "n"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "SSSSSS"
  }

  //future
  import  scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    "a sasas"
  }

  aFuture.onComplete {
    case Success(meaning) => println(s"I found $meaning")
    case Failure(exception) => println(s"I have found $exception")
  }

  println(aFuture.value)

//  partial functions
  val aValue: PartialFunction[Int,String] = {
    case 1 => "S"
    case 2 => "H"
    case _ => "Nothing"
  }

  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x:Int) = x + 43
  implicit val implicitValue: Int = 67
  val implicitcall = methodWithImplicitArgument

  println(s"1.$implicitcall " + s" 2. $implicitValue ")

  case class Person(name: String) {
    def greet = println(s"Hi, My name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "HARSHANG".greet

  // implicit conversion - implicit classes

  implicit class Human(name: String) {
    def greeting = println(s"Hello My name is $name")
  }
  "HARSHANG".greeting
  println(List(1,3,4,2).sorted)

}
