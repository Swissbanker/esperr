\name{esperr-package}
\alias{esperr-package}
\alias{esperr}
\docType{package}
\title{Esper stream processing library for R}
\description{
Esper is a high-performance event stream processing framework. 
The esperr package for R incorporates the Esper framework and implements 
an R-language interface to its XML event API. Events are defined by
XML documents that follow an XML schema definition (XSD) document.
}
\author{
B. W. Lewis and Michael Kane.

Maintainer: B. W. Lewis <blewis@illposed.net>
}
\references{
http://esper.codehaus.org
}
\keyword{ package }
\examples{
# The package includes a sample event schema and xml event taken from
# the Esper Reference Manual, Section 2.7.

require('esperr')

# Configure Esper to use the sample schema. An event schema is required.
# The 'Sensor' parameter is the document root node name (also required).
schema <- paste(system.file(package='esperr'),"/schema/sensor.xsd",sep='')
esperSchema(schema,'Sensor')

# The sensorXMLEvent represents an example event
data(sensorXMLEvent)
cat(sensorXMLEvent)

# Construct a sample stream query statement.
s <- esperStatement('select ID,Observation.Command from MyEvents')

# Define an R event-handling function. The function will be called
# whenever an event meets the query criteria.
f <- function(event) {
  id <- getEventString(event, 'ID')
  command <- getEventString(event,'Observation.Command')
  cat('The event ID property is: ',id,'\n')
  cat('The event Oservation.command property is: ',command,'\n')
  rm(event)
}

# Register the event-handler function with the query statement.
registerEventListener(s, f)

# Note that Esper supports multiple simultaneous query statements, each
# supporting multiple event handlers. Event handler functions may be
# registered more than once, and with multiple statements as well.

# Finally, send an example event that satisfies the query statement and
# triggers the event handler.

sendEvent(sensorXMLEvent)
}
