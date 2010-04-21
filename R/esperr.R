.env <- new.env()

.onLoad <- function (libname, pkgname)
{
  require ("rJava")
  cp <- paste(system.file(package='rJava'),"/java",sep="")
  .jpackage(pkgname,morePaths=cp,lib.loc=libname)
  re <- .jengine(TRUE)
  assign("rengine",re,envir=.env)
  esper <- .jnew("net/illposed/esperr/REP",re)
  assign("esper",esper,envir=.env)
}

.esper <- function()
{
  get("esper",envir=.env)
}

esperConfigureBean <- function(eventName, object)
{
  .jcall(.esper(),"V","configureBean",eventName, .jcast(object))
}

esperXMLEventSchema <- function(file, rootName, eventName="MyEvents")
{
  url <- file
  if(!grepl(':',url)) url <- paste('file:',url,sep='')
  .jcall(.esper(),"V","configureXMLEvent",url,rootName,eventName)
}

esperStatement <- function(string)
{
  list(statement=string,esperObject=.jcall(.esper(),"Lnet/illposed/esperr/Statement;","newStatement",string))
}

# Every event listener stores his events in the global environment.
# The JRI rni interface seems to have trouble storing data in environments:(
registerEventListener <- function(statement, callback, prefix=sub('/','',tempfile(pattern='event',tmpdir='')))
{
  x <- match.call()
  fname <- as.character(x['callback'])
  .jcall(.esper(),"V","addEventListener",statement$esperObject,prefix,fname)
}

getEventString <- function(event, property)
{
  obj <- .jcall(event,"Ljava/lang/Object;","get",property)
  return(.jcall(obj,"S","toString"))
}

destroyAllStatements <- function()
{
  .jcall(.esper(),"V","destroyAllStatements")
}

sendEvent <- function(event)
{
  .jcall(.esper(),"V","sendEvent",as.character(event))
}

sendBean <- function(stream, object)
{
  .jcall(.esper(),"V","sendEvent",as.character(stream), .jcast(object))
}

xmlListener <- function(port=9595, magic="###STOP###", root)
{
  .jcall(.esper(), "V", "streamListener", as.integer(port), as.character(root), as.character(magic))
}

textListener <- function(port=9595, streamToken="stream", delim=",", magic="###STOP###")
{
  .jcall(.esper(), "V", "textServer", as.integer(port), as.character(delim), as.character(streamToken), as.character(magic));
}

esperRedisConnect <- function(host='localhost', port=6379)
{
  .jcall(.esper(),"V","redisConnect",as.character(host),as.integer(port))
}

registerRedisEventListener <- function(statement, key=sub('/','',tempfile(pattern='event',tmpdir='')))
{
  .jcall(.esper(),"V","addRedisEventListener",statement$esperObject,key)
}

