public class StockTick implements java.io.Serializable
{
  private String symbol;
  private Double price;
  public StockTick ()
  {
    symbol = null;
    price = null;
  }
  public StockTick (String symbol, double price)
  {
    this.symbol = symbol;
    this.price = price;
  }
  public double getPrice ()
  {
    return this.price;
  }
  public String getSymbol ()
  {
    return this.symbol;
  }
  public void setPrice (double d)
  {
    this.price = d;
  }
  public void setPrice (String d)
  {
    this.price = (new Double(d)).doubleValue();
  }
  public void setSymbol (String s)
  {
    this.symbol = s;
  }
  public String toString ()
  {
    return (symbol + " " + price);
  }
}
