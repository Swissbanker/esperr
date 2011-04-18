\name{textListener}
\alias{textListener}
\title{
Start a super-basic event stream listener on a TCP/IP port for use
with event beans.
}
\description{
An eventStream listener listens for incoming stream-styly events on a socket,
feeding them into the Esper engine for processing.
}
\usage{
textListener(port = 9595, streamToken="stream", delim=",", magic = "###STOP###")
}
\arguments{
  \item{port}{
The port on which to listen.
}
  \item{streamToken}{The name of the root object class.  }
  \item{delim}{Field delimiter character. }
  \item{magic}{
Terminate the event stream listener when this string is encountered.
}
}
\details{
Use an event stream listener to feed external text stream 
events arriving on a socket
into the Esper engine. The controlling R process will block until the
'magic' token is observed, but will callback
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
bean <- paste(system.file(package='esperr'),"/schema/StockTick.class",sep='')
beanS <- paste(system.file(package='esperr'),"/schema/StockTick.java",sep='')
cat("HERE IS THE JAVA EVENT BEAN CLASS DEFINITION FOR THIS EXAMPLE:\n")
cat(paste(readLines(beanS),collapse="\n"))
cat("\n\n")
file.copy(bean, getwd(), overwrite=TRUE)
.jaddClassPath(getwd())   # Make sure that the bean is in our class path

esperConfigureClass("StockTick", "ticks")
s = esperStatement("select * from ticks where symbol='MSFT'")
f <- function(event) cat(getEventString(event,"price"),"\n")
registerEventListener(s,f)
textListener(9595)
# Now, the R process waits for events on port 9595.

# Open a different R session and run:
con <- socketConnection(port = 9595, blocking = TRUE)
p <- c(500,30,16)
j <- 1
while(j<100)
{
  p <- p + rnorm(3)/100
  m1 <- paste("stream=ticks,symbol=GOOG,price=",p[1],sep="")
  m2 <- paste("stream=ticks,symbol=MSFT,price=",p[2],sep="")
  m3 <- paste("stream=ticks,symbol=UAM,price=",p[3],sep="")
  m <- paste(m1,m2,m3,sep="\n")
  writeLines(m, con)
  j <- j + 1
}
writeLines("###STOP###",con)
close(con)
# You should see the R process callback function respond to the incoming events
}}
