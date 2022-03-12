# Logging with Log4j

Who so ever has worked with any sort of Java application should atleast heard about if not used Log4j extensively. Its simple its flexible and extremely widely used.
In this article will show you how to use Log4j in your PySpark Application.

## Step 1: Creating a simple Log4j Property file

```
log4j.rootLogger=${root.logger}
root.logger=INFO,console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n
shell.log.level=WARN
log4j.logger.org.eclipse.jetty=WARN
log4j.logger.org.spark-project.jetty=WARN
log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
log4j.logger.org.apache.spark.repl.Main=${shell.log.level}
log4j.logger.org.apache.spark.api.python.PythonGatewayServer=${shell.log.level}

# Application Log
log4j.logger.com.sanmuk.spark.project01=DEBUG, console
log4j.additivity.com.sanmuk.spark.project01=false
```

> Note
> - This is the example log4j property i got from Spark config directory. So it has all nessesory defaults.
> - I added my application class (i.e. com.sanmuk.spark.project01) and its logging requirement
> - This article is not about Log4j so i dont go in details and what all more you can do


# Step 2: Define Logger

Now lets define our Spark application with a custom log handler which uses Log4j

```python
from pyspark import SparkContext                                 
                                                                 
class Logging:                                                   
    """PySpark Logger"""                                         
    def __init__(self, sc):                                      
        """Class Initializer"""                                  
        rc = "com.sanmuk.spark.project01"                        
        an = sc._conf.get("spark.app.name")                      
        lg = sc._gateway.jvm.org.apache.log4j                    
        self.logger = lg.LogManager.getLogger(rc + "." + an)     
                                                                 
    def debug(self, mssg):                                       
        """Debug Logging"""                                      
        self.logger.debug(mssg)                                  
                                                                 
    def info(self, mssg):                                        
        """Info Logging"""                                       
        self.logger.info(mssg)                                   
                                                                 
    def warn(self, mssg):                                        
        """Warn Logging"""                                       
        self.logger.warn(mssg)                                   
                                                                 
    def error(self, mssg):                                       
        """Error Logging"""                                      
        self.logger.error(mssg)                                  
                                                                 
                                                                 
sc = SparkContext()                                              
logger = Logging(sc)                                             
                                                                 
logger.debug("Test Debug Log")                                   
logger.info("Test Info Log")                                     
logger.warn("Test Warn Log")                                     
logger.error("Test Error Log")                                   
                                                                 
sc.stop()                                                        
```

> Note
> - Our custom Logging class which does the magic.
> - Here i was using an old Cloudera Hadoop distribution having Spark 1.6, thus initialized SparkContext, hope once can easy port this to Spark 2 or 3.


## Step 3: Execution

Now lets run in Debug Logging:

```
spark-submit --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:log4j.properties' --files log4j.properties test_logging.py
```

> Note
> - Application code above is named 'test_logging.py' and Log4j property file named 'log4j.properties'
> - Files directive added to distribute the Log4j property file and it is set as the Log4j property for Driver


- Expect the logging

```log
22/03/11 22:02:36 DEBUG test_logging.py: Test Debug Log
22/03/11 22:02:36 INFO test_logging.py: Test Info Log
22/03/11 22:02:36 WARN test_logging.py: Test Warn Log
22/03/11 22:02:36 ERROR test_logging.py: Test Error Log
```


***Happy Logging!***
