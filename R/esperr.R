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

esperInit <- function()
{
  rm("esper",envir=.env)
  rep <- .jnew("REP",re)
  assign("esper",rep,envir=.env)
}

esperSchema <- function(file, rootName, eventName="MyEvents")
{
  url <- file
  if(!grepl(':',url)) url <- paste('file:',url,sep='')
  .jcall(.esper(),"V","setup",url,rootName,eventName)
}

esperStatement <- function(string)
{
  list(statement=string,esperObject=.jcall(.esper(),"Lnet/illposed/esperr/Statement;","newStatement",string))
}

# Every event listener stores his events in the global environment.
# The JRI rni interface seems to have trouble storing data in environments:(
registerEventListener <- function(statement, callback, prefix=sub('/','',tempfile(pattern='event',tmpdir='')))
{
  .jcall(.esper(),"V","addEventListener",statement$esperObject,prefix,callback)
}

getEventString <- function(event, property)
{
  obj <- .jcall(event,"Ljava/lang/Object;","get",property)
  return(.jcall(obj,"S","toString"))
}

sendEvent <- function(event)
{
  .jcall(.esper(),"V","sendEvent",as.character(event))
}

streamServer <- function(port, root, magic)
{
  .jcall(.esper(),"V", "streamListener", as.integer(port), 
           as.character(root), as.character(magic));
}
