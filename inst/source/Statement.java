package net.illposed.esperr;

import com.espertech.esper.client.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Statement
  {
  private EPStatement statement;
  String string = null;
  public Statement(EPAdministrator admin, String s)
  {
    string = s;
    statement = admin.createEPL(s);
  }
  public String getStatement() {return(string);}
  public void addListener(UpdateListener listener)
  {
    statement.addListener(listener);
  }
}
