JRI=$$(R --vanilla --slave -e "cat (system.file(package='rJava'))")

CLASSPATH=".:../java/esper-3.4.0.jar:../java/cglib-nodep-2.2.jar:../java/commons-logging-1.1.1.jar:../java/log4j-1.2.15.jar:../java/antlr-runtime-3.1.1.jar:../java/esperio-socket-3.4.0.jar:../java/esperio-http-3.4.0.jar:../java/httpcore-nio-4.0.1.jar:./net/illposed/esperr/:$(JRI)/jri/JRI.jar"

REP:
	javac -Xlint -cp $(CLASSPATH) -source 1.5 -target 1.5 -d . *.java
	jar -cf REP.jar  ./net/illposed/esperr/*.class
	cp REP.jar ../java/

clean:
	rm -rf *.class net
	rm -rf  net
	mkdir -p net/illposed/esperr
