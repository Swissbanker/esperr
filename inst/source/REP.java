package net.illposed.esperr;

import net.illposed.esperr.*;

import com.espertech.esper.client.*;
import com.espertech.esper.event.*;
import com.espertech.esper.event.bean.*;
import com.espertech.esperio.socket.*;
import com.espertech.esperio.socket.config.*;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.EPServiceProviderSPI;
import com.espertech.esper.event.EventBeanManufactureException;
import com.espertech.esper.event.EventAdapterServiceImpl;
import com.espertech.esper.event.EventBeanManufacturer;
import com.espertech.esper.event.EventTypeSPI;
import com.espertech.esper.event.WriteablePropertyDescriptor;
import com.espertech.esper.util.SimpleTypeParser;
import com.espertech.esper.util.SimpleTypeParserFactory;
import com.espertech.esper.epl.core.MethodResolutionServiceImpl;

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
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import javax.xml.parsers.*;
import java.lang.reflect.*;

public class REP
{
  public Rengine re;
  public EPServiceProvider epService;
  public EsperIOSocketAdapter socketAdapter;
  private Log log = LogFactory.getLog (REP.class);
  private BasicRedis redisClient;

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
// Generic engine configuration
  public void configureBean (String eventName, Object obj)
  {
    epService = EPServiceProviderManager.getDefaultProvider ();
    try
    {
      epService.getEPAdministrator ().
        getConfiguration ().addEventType (eventName, obj.getClass ());
    }
    catch (Exception ex)
    {
      System.err.println (ex.toString ());
    }
  }
// Setup EP for XML events
  public void configureXMLEvent (String schema, String rootName,
                                 String eventName)
  {
    epService = EPServiceProviderManager.getDefaultProvider ();
    ConfigurationEventTypeXMLDOM epcfg = new ConfigurationEventTypeXMLDOM ();
    epcfg.setRootElementName (rootName);
    epcfg.setSchemaResource (schema);
    epService.getEPAdministrator ().
      getConfiguration ().addEventType (eventName, epcfg);
  }
  public void destroyAllStatements ()
  {
    epService.getEPAdministrator ().destroyAllStatements ();
  }
// Call this from R to create a new statement
  public Statement newStatement (String query)
  {
    Statement s = new Statement (epService.getEPAdministrator (), query);
    return (s);
  }
// Call this from R to add an eventListener to a statement
  public void addEventListener (Statement s, String prefix, String callback)
  {
    s.addListener (new UL (callback, prefix));
  }
// Call this from R to send an XML event (useful for testing)
  public void sendEvent (String xml)
  {
    DocumentBuilderFactory builderFactory;
    Document doc = null;
    InputSource source = new InputSource (new StringReader (xml));
    builderFactory = DocumentBuilderFactory.newInstance ();
    builderFactory.setNamespaceAware (true);
    try
    {
      doc = builderFactory.newDocumentBuilder ().parse (source);
    } catch (Exception ex)
    {
    };
    epService.getEPRuntime ().sendEvent (doc);
  }
  // Generic Bean event sender to specified stream
  public void sendEvent (String stream, Object event)
  {
    EventSender sender = epService.getEPRuntime ().getEventSender (stream);
    sender.sendEvent (event);
  }

/* 
 * Experimental methods
 */

// This is an ultra-basic method that listens for XML events on a port.
// The method blocks the controlling R process (aside from callbacks) until
// the "magic" string is received.
  class xmlServer
  {
    public xmlServer (int port, String root,
                         String magic) throws IOException
    {
      boolean run = true;
      String termini = "</" + root + ">";
      String s;
      ServerSocket ss = new ServerSocket (port);
        System.out.println ("Waiting for XML events on port " + port);
      while (run)
        {
          Socket cs = ss.accept ();
          StringBuffer sb = new StringBuffer ();
          BufferedReader data = new
            BufferedReader (new InputStreamReader (cs.getInputStream ()));
            s = data.readLine ();
          while (s != null)
            {
              sb.append (s);
              if (s.contains (magic))
                run = false;
              if (s.contains (termini))
                {
                  sendEvent (sb.toString ());
                  sb.delete (0, sb.length ());
                }
              s = data.readLine ();
            }
          data.close ();
          cs.close ();
        }
      ss.close ();
    }
  }

  public void xmlListener (int port, String root, String magic)
  {
    try
    {
      xmlServer ss = new xmlServer (port, root, magic);
    } catch (Exception ex)
    {
      System.out.println (ex.toString ());
    }
  }


// XXX There is a method available in Esper already for this...
  public String eventToDoc (EventBean eb)
  {
    StringBuffer sb = new StringBuffer ();
    String[]en = eb.getEventType ().getPropertyNames ();
    EventBeanReaderDefaultImpl er = new
      EventBeanReaderDefaultImpl (eb.getEventType ());
    Object[]ob = er.read (eb);
    for (int j = 0; j < en.length; ++j)
      {
        sb.append ("<" + en[j] + ">");
        sb.append (ob[j].toString ());
        sb.append ("</" + en[j] + ">\n");
      }
    return (sb.toString ());
  }

  public void redisConnect (String host, int port)
  {
    try
    {
      redisClient = new BasicRedis (host, port);
    }
    catch (Exception ex)
    {
      System.err.println ("redisConnect error");
    }
  }

  public void addRedisEventListener (Statement s, String key)
  {
    s.addListener (new RL (key));
  }

  public class RL implements UpdateListener
  {
    String key = null;
    public RL (String s)
    {
      key = s;
    }

    public void update (EventBean[]newEvents, EventBean[]oldEvents)
    {
      if (newEvents.length == 1 && oldEvents == null)
        {
          try
          {
            redisClient.set (key, eventToDoc (newEvents[0]));
          }
          catch (Exception ex)
          {
          }
          return;
        }
    }
  }

  public void spigot (int rep)
  {
    String rareEvent =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Sensor xmlns=\"SensorSchema\">\n<ID>homer</ID>\n<Observation Command=\"READ_PALLET_TAGS_ONLY\">\n    <ID>00000001</ID>\n    <Tag> <ID>urn:epc:1:2.24.400</ID> </Tag>\n    <Tag> <ID>urn:epc:1:2.24.401</ID> </Tag>\n</Observation>\n</Sensor>\n";
    String commonEvent =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Sensor xmlns=\"SensorSchema\">\n<ID>urn:epc:1:4.16.36</ID>\n<Observation Command=\"READ_PALLET_TAGS_ONLY\">\n    <ID>00000001</ID>\n    <Tag> <ID>urn:epc:1:2.24.400</ID> </Tag>\n    <Tag> <ID>urn:epc:1:2.24.401</ID> </Tag>\n</Observation>\n</Sensor>\n";
    for (int k = 1; k < rep; ++k)
      {
        for (int j = 1; j < 1000; ++j)
          sendEvent (commonEvent);
        sendEvent (rareEvent);
      }
  }

  public void spigot2 (int m, int n, String stream, Object rare,
                       Object common)
  {
    for (int k = 1; k < m; ++k)
      {
        for (int j = 1; j < n; ++j)
          sendEvent (stream, common);
        sendEvent (stream, rare);
      }
  }


// This is an ultra-basic method that listens for text events on a port.
// Text event formatting is similar to the "socket" method in esperio:
// stream=<name>,property=<value>,...
// with one event per line. It's a little more flexible than esperio since
// the delimiter and stream token can be set.
// I'm using java reflection here, as I had trouble with the default Esper
// Event manufacturer methods. The fast bean manufacturer methods are preferred.
// The method blocks the controlling R process (aside from callbacks) until
// the "magic" string is received.
  public void textServer (int port, String delim, String streamToken, String magic) throws IOException
  {
    EPServiceProviderSPI engine = (EPServiceProviderSPI) epService;
    Class<?> stringClass = (new String()).getClass();
    boolean run = true;
    String s;
    ServerSocket ss = new ServerSocket (port);
      System.out.println ("Waiting for x events on port " + port);
    while (run)
      {
        Socket cs = ss.accept ();
        BufferedReader data = new
          BufferedReader (new InputStreamReader (cs.getInputStream ()));
          s = data.readLine ();
        while (s != null)
          {
            if(processStringEvent(s,delim,streamToken,magic)){
              run=false;
              break;
            }
            s = data.readLine ();
          }
        data.close ();
        cs.close ();
      }
    ss.close ();
  }

  boolean processStringEvent(String text, String delim, String streamToken, String magic)
  {
    if(text == null) return false;
    EPServiceProviderSPI engine = (EPServiceProviderSPI) epService;
    Class<?> stringClass = (new String()).getClass();
    Map <String,String> parse = new HashMap<String,String>();
    try {
      String[] s = text.split(delim);
      for(int j=0;j<s.length;++j) {
        if(s[j].contains(magic)) return true;
        String[] r = s[j].split("=");
// XXX We use a questionably-strict setter convention here:
        if(j>0) r[0] = "set" + r[0].substring(0,1).toUpperCase() 
                           + r[0].substring(1);
        parse.put(r[0],r[1]);
      }
    } catch (Exception ex) {return false;}
    try {
      String eventTypeName = parse.get(streamToken);
      parse.remove(streamToken);
      Class<?> c = null;
      EventSender sender = epService.getEPRuntime().getEventSender(eventTypeName);
      EventType eventType = engine.getEventAdapterService().getExistsTypeByName(eventTypeName);
      c = eventType.getUnderlyingType();
      Object event = c.newInstance();
      String setter;
      for (Iterator it=parse.keySet().iterator(); it.hasNext(); ) {
        setter = (String)it.next();
        Method n = c.getDeclaredMethod(setter,stringClass);
        n.invoke(event, parse.get(setter));
      }
      sender.sendEvent (event);
    } catch (Throwable e) { System.err.println(e); }
    return false;
  }



/* XXX End of experimental stuff */

/* The following classes and functions are internal to REP */

// The basic R event callback wrapper
  public class UL implements UpdateListener
  {
    String callback = null;
    String prefix = null;
    public UL (String s, String v)
    {
      callback = s;
      prefix = v;
    }

// Publish the events to R global environment and invoke the callback function
// XXX add oldEvents and improve this scheme
    public void update (EventBean[]newEvents, EventBean[]oldEvents)
    {
      if (newEvents.length == 1 && oldEvents == null)
        {
          String v = prefix;
          REXP revent = re.createRJavaRef (newEvents[0]);
          re.assign (v, revent);
          re.eval (callback + "(" + v + ")");
          return;
        }
      if (newEvents.length == 1 && oldEvents.length == 1)
        {
          String v = prefix + ".new";
          REXP revent = re.createRJavaRef (newEvents[0]);
          re.assign (v, revent);
          v = prefix + ".old";
          REXP orevent = re.createRJavaRef (oldEvents[0]);
          re.assign (v, orevent);
          re.eval (callback + "('" + v + "')");
          return;
        }
      for (int j = 0; j < newEvents.length; ++j)
        {
          String v = prefix + ".new." + j;
          REXP revent = re.createRJavaRef (newEvents[j]);
          re.assign (v, revent);
        }
      if (oldEvents != null)
        {
          for (int j = 0; j < newEvents.length; ++j)
            {
              String v = prefix + ".old." + j;
              REXP revent = re.createRJavaRef (oldEvents[j]);
              re.assign (v, revent);
            }
        }
      re.eval (callback + "('" + prefix + "')");
    }
  }


}


/* XXX Experimental classes */

/* An ultra-basic Redis client class for storing output events. */
class BasicRedis
{
  Socket clientSocket;

  public BasicRedis (String host, int port) throws java.io.IOException
  {
    clientSocket = new Socket (host, port);
  }

  public String set (String key, String value) throws java.io.IOException
  {
    DataOutputStream os =
      new DataOutputStream (clientSocket.getOutputStream ());
    BufferedReader is =
      new BufferedReader (new
                          InputStreamReader (clientSocket.getInputStream ()));
    String msg =
      "SET " + key + " " + value.length () + "\r\n" + value + "\r\n";
      os.writeBytes (msg);
      return (is.readLine ());
  }

  public void close () throws java.io.IOException
  {
    clientSocket.close ();
  }

}


