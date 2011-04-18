\name{esperConfigureObject}
\alias{esperConfigureObject}
\title{
Configure an event bean schema from an instance of a class.
}
\description{
Configure an event bean schema from an instance of a class.
}
\usage{
esperConfigureObject(object, eventName = "MyEvents")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
An instance of an event bean class.
}
  \item{eventName}{
The schema name.
}
}
\value{
Nothing is returned
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\author{
B. W. Lewis <blewis@illposed.net>
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function(object, eventName="MyEvents")
{
  .jcall(.esper(),"V","configureBean",eventName, .jcast(object))
  }
}
