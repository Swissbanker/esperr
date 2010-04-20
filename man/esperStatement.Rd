\name{esperStatement}
\alias{esperStatement}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Create an Esper EPL statement
}
\description{
EPL is an SQL-like stream processing query language. Use esperStatement
to create statements that can be registered with output event listeners.
}
\usage{
esperStatement(string)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{string}{
The statement string (see the examples).
}
}
\value{
An Esper statement object (a list).
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
