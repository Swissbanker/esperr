\name{registerEventListener}
\alias{registerEventListener}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Register an R function to consume output events.
}
\description{
Register an R function with an Esper EPL statement. The specified R function
will be called whenever an event is output from the EPL statement.
}
\usage{
registerEventListener(statement, callback, prefix = sub("/", "", tempfile(pattern = "event", tmpdir = "")))
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{statement}{
An EPL statement created with \code{esperStatement}
}
  \item{callback}{
An R function of one argument corresponding to the output event
}
  \item{prefix}{
Output events for R function callbacks are stored in the global environment for
unfortunate technical reasons (hopefully not for long).
They are assigned a temporary name with the givent prefix.
}
}
\details{
The R callback function must have one argument. The argument corresponds to
the event. The function may parse the event property values with the 
\code{getEventString} function. The function should also remove the event
from the global environment upon completion, as shown in the examples. We're
working on improving this callback behavior.
}
\value{
Nothing is returned.
}
\author{
B. W. Lewis
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{registerRedisEventListener}
}
\examples{
require('esperr')
example(esperr)
}
