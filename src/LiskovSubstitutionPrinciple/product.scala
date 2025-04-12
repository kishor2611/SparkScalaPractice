//package LiskovSubstitutionPrinciple
//
//trait product {
//  def price: Double
//  def description: String
//}
//
//class RefrigeratedProducts(name:String, price: Double,temp: Double) extends product {
//  def temperature: Double = temp
//  def isRefrigerated: Boolean = true
//
//  override def description: String = ???
//
//  override def price: Double = ???
//}
//
//trait NonRefrigeratedProducts extends product{
//
//  def packagingType: String
//}
//
//class Milk(override val price: Double, override val description: String,
//           override val temperature: Double) extends RefrigeratedProducts {
//  override def toString: String = s"Milk(price=$price, description=$description, temperature=$temperature)"
//}
//
//class Cereal(val price: Double, val description: String, val packagingType: String) extends NonRefrigeratedProducts {
//  override def toString: String = s"Cereal(price=$price, description=$description, packagingType=$packagingType)"
//}