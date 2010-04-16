package net.illposed.esperr;

import com.espertech.esper.client.*;
import com.espertech.esper.event.*;
import com.espertech.esperio.socket.*;
import com.espertech.esperio.socket.config.*;
import com.espertech.esperio.http.*;
import com.espertech.esperio.http.config.*;
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
import java.io.*;
import javax.xml.parsers.*;

public class REP
{
  public Rengine re;
  public EPServiceProvider epService;
  private Log log = LogFactory.getLog (REP.class);
  private BasicRedis redisClient;
  public EsperIOSocketAdapter socketAdapter;
  public EsperIOHTTPAdapter httpAdapter;

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
    epService = EPServiceProviderManager.getDefaultProvider ();
    ConfigurationEventTypeXMLDOM epcfg = new ConfigurationEventTypeXMLDOM ();
    epcfg.setRootElementName (rootName);
    epcfg.setSchemaResource (schema);
    epService.getEPAdministrator ().getConfiguration ().
      addEventType (eventName, epcfg);
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

/* 
 * Experimental methods
 */

// This is an ultra-basic method that listens for XML events on a port.
// It's really just test code, but does offer reasonable performance.
// The method blocks the controlling R process (aside from callbacks) until
// the "magic" string is received.
class streamServer
{
  public streamServer (int port, String root,
                      String magic) throws IOException
  {
    boolean run = true;
    String termini = "</" + root + ">";
    String s;
    ServerSocket ss = new ServerSocket (port);
    System.out.println ("Waiting for XML events on port "+port);
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

public void streamListener(int port, String root, String magic)
{
  try{
    streamServer ss = new streamServer(port, root, magic);
  } catch(Exception ex){System.err.println(ex.toString());}
}

// Call this from R to start an stream server that listens for events on a port
  public void socketListener (int port)
  {
    try
    {
      ConfigurationSocketAdapter adapterConfig =
        new ConfigurationSocketAdapter ();
      SocketConfig socket = new SocketConfig ();
      socket.setDataType (DataType.CSV);
      socket.setPort (port);
      adapterConfig.getSockets ().put ("socketListener", socket);
      // start adapter
      socketAdapter = new EsperIOSocketAdapter (adapterConfig, "engineURI");
      socketAdapter.start ();
    } catch (Exception e)
    {
      System.err.println ("socketListener error " + e.toString ());
    }
    System.out.println ("OK");
  }

  public void stopSocketListener ()
  {
    try
    {
      socketAdapter.destroy ();
    } catch (Exception e)
    {
      System.err.println (e.toString ());
    }
  }

  public void httpListener (int port)
  {
    ConfigurationHTTPAdapter adapterConfig = new ConfigurationHTTPAdapter ();
    Service service = new Service ();
    service.setPort (port);
    service.setNio (true);
    adapterConfig.getServices ().put ("http", service);
    GetHandler getHandler = new GetHandler ();
    getHandler.setPattern ("*");
    getHandler.setService ("http");
    adapterConfig.getGetHandlers ().add (getHandler);
// start adapter
    httpAdapter = new EsperIOHTTPAdapter (adapterConfig, "engineURI");
    System.out.println ("yikes");
    httpAdapter.start ();
    System.out.println (adapterConfig.toString ());
  }

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
