package net.illposed.esperr;

import com.espertech.esper.client.*;
import net.illposed.esperr.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.*;
import org.w3c.dom.Document;
import org.w3c.dom.DOMException;

import org.rosuda.JRI.*;
import org.rosuda.JRI.Rengine;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.RMainLoopCallbacks;
import org.rosuda.JRI.RConsoleOutputStream;

import java.util.LinkedList;
import java.net.*;
import java.io.StringReader;
import javax.xml.parsers.*;

public class REP
{
  public Rengine re;
  public EPServiceProvider epService;
  private Log log = LogFactory.getLog(REP.class);

/* R interface functions
 * The program proceeds as follows:
 * 1. Initialie the engine
 * 2. Setup the engine with an event schema XML string
 * 3. Create R event listener callback functions
 * 4. Create an EP statement
 * 5. Add event listeners from 3 to the statement
 * 6. Start sending events.
 */
// Initialize REP with an instance of the running R engine (use .jengine)
  public REP (Rengine R)
  {
    re = R;
  }
// Setup the EP
  public void setup (String schema, String rootName, String eventName)
   {
    System.out.println ("setup EP");
    epService = EPServiceProviderManager.getDefaultProvider();
    ConfigurationEventTypeXMLDOM epcfg = new ConfigurationEventTypeXMLDOM();
    epcfg.setRootElementName(rootName);
    epcfg.setSchemaResource(schema);
    epService.getEPAdministrator().getConfiguration()
      .addEventType(eventName, epcfg);
    System.out.println ("setup EP done");
   }
// Call this from R to create a new statement
  public Statement newStatement(String query)
   {
    Statement s = new Statement(epService.getEPAdministrator(), query);
    return(s);
   }
// Call this from R to add an eventListener to a statement
  public void addEventListener(Statement s, String prefix, String callback)
   {
    s.addListener(new UL(callback, prefix));
   }
// Call this from R to send an XML event. Alternatively, process events
// directly in Java.
  public void sendEvent(String xml)
   {
    DocumentBuilderFactory builderFactory;
    Document doc = null;
    InputSource source = new InputSource(new StringReader(xml));
    builderFactory = DocumentBuilderFactory.newInstance();
    builderFactory.setNamespaceAware(true);
    try{
      doc = builderFactory.newDocumentBuilder().parse(source);
    } catch(Exception ex){};
    epService.getEPRuntime().sendEvent(doc);
   }
// XXX Add an "eventServer" method that processes events from a tcp connection


/* The following classes and functions are internal to REP */
  public class UL implements UpdateListener
  {
    String callback = null;
    String prefix = null;
    public UL(String s, String v){
      callback = s;
      prefix = v;
    }

// Publish the events to R global environment and invoke the callback function
// XXX add oldEvents and improve this lame scheme
    public void update(EventBean[] newEvents, EventBean[] oldEvents)
    {
      if(newEvents.length==1 && oldEvents==null) {
        String v = prefix;
        REXP revent = re.createRJavaRef(newEvents[0]);
        re.assign(v,revent);
        re.eval(callback + "(" + v + ")");
        return;
      }
      if(newEvents.length==1 && oldEvents.length==1) {
        String v = prefix + ".new";
        REXP revent = re.createRJavaRef(newEvents[0]);
        re.assign(v,revent);
        v = prefix + ".old";
        REXP orevent = re.createRJavaRef(oldEvents[0]);
        re.assign(v,orevent);
        re.eval(callback + "('" + v + "')");
        return;
      }
      for(int j=0;j<newEvents.length;++j) {
        String v = prefix + ".new." + j;
        REXP revent = re.createRJavaRef(newEvents[j]);
        re.assign(v,revent);
      }
      if(oldEvents!=null) {
        for(int j=0;j<newEvents.length;++j) {
          String v = prefix + ".old." + j;
          REXP revent = re.createRJavaRef(oldEvents[j]);
          re.assign(v,revent);
        }
      }
      re.eval(callback + "('" + prefix + "')");
    }
  }
}
