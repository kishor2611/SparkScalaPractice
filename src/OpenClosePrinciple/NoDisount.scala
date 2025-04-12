package OpenClosePrinciple

class NoDisount extends Discount{
  override def apply(price: Double): Double = price

}
