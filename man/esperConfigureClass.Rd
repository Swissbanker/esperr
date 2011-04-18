\name{esperConfigureClass}
\alias{esperConfigureClass}
\title{
Configure an Event Bean class
}
\description{
Configure and name the event schema from an Event Bean class name. The class
must be in the Java CLASSPATH.
}
\usage{
esperConfigureClass(className, eventName = "MyEvents")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{className}{
The Java class name.
}
  \item{eventName}{
A name refering to the event bean schema.
}
}
\details{
Make sure that the class is in the Java CLASSPATH, for example, use
\code{.jaddClassPath(getwd())} or similar command first.
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
B. W. Lewis <blewis@illposed.net>
}
\examples{
bean <- paste(system.file(package='esperr'),"/schema/StockTick.class",sep='')
file.copy(bean, getwd(), overwrite=TRUE)
.jaddClassPath(getwd())   # Make sure that the bean is in our class path
esperConfigureClass("StockTick", "ticks")
}
