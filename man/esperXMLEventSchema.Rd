\name{esperXMLEventSchema}
\alias{esperXMLEventSchema}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Configure XML Event Schema}
\description{
Use esperXMLEventSchema to specify an event XML schema document (XSD) file and
associate the event schema with a stream name.
}
\usage{
esperXMLEventSchema(file, rootName, eventName = "MyEvents")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{file}{
The XML schema document file or URI.
}
  \item{rootName}{
The XML root node identifier.
}
  \item{eventName}{
The name of the stream associated with this schema. The default name is
'MyEvents.'
}
}
\details{
Esper XML events are defined by XML schema documents (XSD). 
See the examples below
for an example XSD. Each event schema is
associated with an 'eventName' identifier, for use in EPL query statements.
The schema document may be a local file or a URL location.
}
\value{
Nothing is returned.
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\author{
B. W. Lewis
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{esperStatement}}
}
\examples{
require('esperr')
schema <- paste(system.file(package='esperr'),"/schema/sensor.xsd",sep='')
# Let's look at this schema document:
cat(paste(readLines(schema),collapse="\n"))
# Finally, register this stream as "SensorEvents"
esperXMLEventSchema(schema,'Sensor',eventName="SensorEvents")
}
