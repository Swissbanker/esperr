\name{getEventString}
\alias{getEventString}
\title{
Retrieve a string representation of an event property.
}
\description{
Event properties are defined by the event XML schema document.
}
\usage{
getEventString(event, property)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{event}{
An Esper event.
}
  \item{property}{
The property name.
}
}
\details{
Use getEventProperties to list all the event properties.
}
\value{
A string representation of the value.
}
\author{
B. W. Lewis
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{registerEventListener}}
}
\examples{
require('esperr')
example(esperr)
}
