package mathOperations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object mathOperarionDup {

  def main(args:Array[String]):Unit={

    val spark= SparkSession.builder().appName("MathOperation").master("local[*]").getOrCreate()
    import spark.implicits._

    val numbers:List[Int]= List(1,2,3,4,5,6,7,8,9,10)

    println("sum of Squared numbers:"+sumOfSquaredNumbers(numbers))
    println("Squared numbers:"+squaredNumbers(numbers))
    println("extract even numbers:"+extractEvenNumbers(numbers))
    println("extract odd numbers:"+extractOddNumbers(numbers))
    println("factorials numbers:"+factorialNumbers(numbers))
    println("Doubled numbers:"+ numbers.map(_*2))

  }

  def sumOfSquaredNumbers(numbers:List[Int]):Int={
    var result=0
    for(num <- numbers){
      result += num*num
    }
    result
  }

  def squaredNumbers(numbers:List[Int]):List[Int]={
    var squareNumbers :List[Int] = List()
    for(num<-numbers){
      squareNumbers ::= num*num
    }
    squareNumbers.reverse
  }

  def extractEvenNumbers(numbers:List[Int]):List[Int]={
    var evenNumbers:List[Int]=List()
    for(num<-numbers){
      if(num%2==0)
        evenNumbers ::= num
    }
    evenNumbers.reverse
  }

  def extractOddNumbers(numbers:List[Int]):List[Int]={
    var oddnumbers:List[Int]=List()
    for(num<-numbers){
      if(num%2!=0)
        oddnumbers ::=num
    }
    oddnumbers.reverse
  }

  def factorialCalc(num:Int):Int={
    if(num==0)
      1
    else
      num*factorialCalc(num-1)
  }

  def factorialNumbers(numbers:List[Int]):List[Int]={
    var factorialNumers:List[Int]=List()
    for(num<- numbers){
      factorialNumers ::= factorialCalc(num)
    }
    factorialNumers.reverse
  }


}
