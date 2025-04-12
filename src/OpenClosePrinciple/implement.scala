package OpenClosePrinciple

object implement {
  def calculateDiscount(price: Double, discount: Discount): Double = {
    discount.apply(price)
  }

  def main(args: Array[String]): Unit = {
    val holiday = new Holiday
    val noDiscount = new NoDisount
    println("Holiday discount "+ calculateDiscount(100, holiday))
    println("No discount "+ calculateDiscount(100, noDiscount))
  }
}

