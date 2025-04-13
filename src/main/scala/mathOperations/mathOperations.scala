package mathOperations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import utilFunctions.commonFunctions.getSparkSession

object mathOperations {

  def sumOfSquare(numbers:List[Int]): Int={
    var sum = 0
    for(num <- numbers){
      var square = num * num
       sum += square
    }
    sum
  }

  def squaredNumbers(numbers:List[Int]):List[Int] ={
    var squaredNumbers:List[Int] = List()
    for(num<- numbers){
      squaredNumbers ::= num*num
    }
    squaredNumbers.reverse
  }

  def extractEvenNumbers(numbers:List[Int]):List[Int]={
    var EvenNumbers:List[Int] = List()
    for (num<- numbers){
      if(num%2 ==0)
        EvenNumbers ::= num
    }
    EvenNumbers.reverse
  }

  def extractOddNumbers(numbers:List[Int]):List[Int]={
    var oddNumbers:List[Int]= List()
    for(num<- numbers){
      if(num%2!=0)
        oddNumbers ::= num
    }
    oddNumbers.reverse
  }

  def factorialNumbers(numbers:List[Int]):List[Int]={
    var factorialList:List[Int]=List()
    for(num<-numbers){
      factorialList ::= factorialCalc(num)
    }
    factorialList.reverse
  }

  def factorialCalc(num:Int): Int = {
    if (num == 0) {
      1
    } else {
      num * factorialCalc(num-1)
    }
  }



  def main(args:Array[String]):Unit={

    val spark=getSparkSession()
    import spark.implicits._

    val numberList:List[Int] = List(1,2,3,4,5,6,7,8,9,10)
    println("Sum of numbers:" + sumOfSquare(numberList))
    println("Squared number:"+ squaredNumbers(numberList))
    println("even number:"+ extractEvenNumbers(numberList))
    println("odd number:"+ extractOddNumbers(numberList))
    println("doubled numbers:"+ numberList.map(_ *2))
    println("Factorial :"+ factorialNumbers(numberList))


  }


}
