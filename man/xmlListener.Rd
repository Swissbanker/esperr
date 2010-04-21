\name{xmlListener}
\alias{xmlListener}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Start a super-basic XML event stream listener on a TCP/IP port.
}
\description{
An eventStream listener listens for incoming XML events on a socket,
feeding them into the Esper engine for processing.
}
\usage{
xmlListener(port = 9595, magic = "###STOP###", root)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{port}{
The port on which to listen.
}
  \item{magic}{
Terminate the event stream listener when this string is encountered.
}
  \item{root}{
The root node of the XML event type to listen for (this basic listener can only
listen for specific event types).
}
}
\details{
Use an event stream listener to feed external XML events arriving on a socket
into the Esper engine. The controlling R process will block, but will callback
to registered R function event listeners.
}
\value{
Nothing is returned.
}
\author{
B. W. Lewis
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\examples{
\dontrun{
require('esperr')
schema <- paste(system.file(package='esperr'),"/schema/sensor.xsd",sep='')
esperXMLEventSchema(schema,'Sensor')
data(sensorXMLEvent)
s <- esperStatement('select ID,Observation.Command from MyEvents')
f <- function(event) {
  id <- getEventString(event, 'ID')
  command <- getEventString(event,'Observation.Command')
  cat('The event ID property is: ',id,'\n')
  cat('The event Oservation.command property is: ',command,'\n')
  rm(event)
}
registerEventListener(s, f)
xmlListener(port = 9595, magic = "###STOP###", root="Sensor")

# Now, the R process waits for events on port 9595.
# Open a terminal window, and type:
cat | nc localhost 9595 << EOF
<?xml version="1.0" encoding="UTF-8"?>
<Sensor xmlns="SensorSchema">
  <ID>urn:epc:1:4.16.36</ID>
  <Observation Command="READ_PALLET_TAGS_ONLY">
    <ID>00000001</ID>
    <Tag>
      <ID>urn:epc:1:2.24.400</ID>
    </Tag>
    <Tag>
      <ID>urn:epc:1:2.24.401</ID>
    </Tag>
  </Observation>
</Sensor>
EOF
# You should see the R process callback function respond to the incoming event.
}}
