\name{destroyAllStatements}
\alias{destroyAllStatements}
\title{Destroy all active statements}
\description{Terminate and remove all active CEP statements. }
\usage{
destroyAllStatements()
}
%- maybe also 'usage' for other objects documented here.
\details{
}
\value{
Nothing is returned.
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
See the Esper documentation.
}
\author{
B. W. Lewis <blewis@illposed.net>
}

\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function()
{
  .jcall(.esper(),"V","destroyAllStatements")
  }
}
