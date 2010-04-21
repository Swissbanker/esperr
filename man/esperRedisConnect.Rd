\name{esperRedisConnect}
\alias{esperRedisConnect}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Connect Esper to a Redis server (Experimental)
}
\description{
This experimental function initiates a connection between Esper
and Redis. 
}
\usage{
esperRedisConnect(host='localhost', port=6379)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{host}{
The Redis host server name or IP address.
}
  \item{port}{
The Redis port number.
}
}
\details{
Redis is a high-performance, in-memory networked key/value database.
We use Redis to cache Esper output events for consumption by 
arbitrary Redis clients without message routing.
Use the registerRedisEventListener function to populate
Redis keys with Esper output events. The cached output events may be 
read by any Redis client.
}
\value{
Nothing is returned.
}
\references{
http://cran.r-project.org/web/packages/rredis/index.html
}
\author{
B. W. Lewis
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{registerRedisEventListener}}
}
\examples{
\dontrun{
# (Assumes a Redis server is running locally)
require('esperr')
schema <- paste(system.file(package='esperr'),"/schema/sensor.xsd",sep='')
esperXMLEventSchema(schema,'Sensor')
data(sensorXMLEvent)
s <- esperStatement('select ID,Observation.Command from MyEvents')
registerRedisEventListener('sensorEvent')
sendEvent(sensorXMLEvent)
# Redis now contains an output event value in the key name 'sensorEvent'
}
}
