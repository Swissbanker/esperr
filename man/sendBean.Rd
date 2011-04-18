\name{sendBean}
\alias{sendBean}
\title{
Send a bean event.
}
\description{
Send an instance of an event bean class into an event stream.
}
\usage{
sendBean(stream, object)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{stream}{
The stream name.
}
  \item{object}{
An instance of an event bean class, configured with esperConfigureClass
or esperConfigureObject.
}
}
\value{
Nothing is returned
}
\author{
B. W. Lewis
}

\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function(stream, object)
{
  .jcall(.esper(),"V","sendEvent",as.character(stream), .jcast(object))
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
