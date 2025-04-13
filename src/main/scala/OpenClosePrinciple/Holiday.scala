package OpenClosePrinciple

class Holiday extends Discount{
  override def apply(price: Double): Double = price*0.5
}
