# Execute Java Method

Sometimes there are requirement for calling Java methods from Pyspark driver program (Note here we are not talking about UDF which execute in Executors). Sometime some implementations are natural with Java also there are ready made third party implentation already available in Java. Lets see how we use Java code from Pyspark driver.

## Create a demo Java function

- Our sample Java function.

```Java
apple@apples-MacBook-Air java % cat com/sanmuk/pyjava/Main.java 
package com.sanmuk.pyjava;

public class Main {

  public static void divNumbers(int a, int b) {
    int sum = a / b;
    System.out.println(sum);
  }

  public static int addNumbers(int a, int b) {
    int sum = a + b;
    return sum;
  }

  public static void main(String[] args) {
    int result;

    System.out.println("Divide 10 with 1 is: ");
    divNumbers(10, 1);
    System.out.println("Add 1 with 4 is: ");
    System.out.println(addNumbers(1, 4));

  }

}
```

- Lets compile and create a Jar.

```shell
apple@apples-MacBook-Air java % javac ./com/sanmuk/pyjava/Main.java
apple@apples-MacBook-Air java % jar cfv test.jar .                                
added manifest
adding: com/(in = 0) (out= 0)(stored 0%)
adding: com/sanmuk/(in = 0) (out= 0)(stored 0%)
adding: com/sanmuk/pyjava/(in = 0) (out= 0)(stored 0%)
adding: com/sanmuk/pyjava/Main.java(in = 468) (out= 228)(deflated 51%)
adding: com/sanmuk/pyjava/Main.class(in = 686) (out= 442)(deflated 35%)
apple@apples-MacBook-Air java % ls -ltr
total 8
drwxr-xr-x  3 apple  staff    96 Aug  9 21:50 com
-rw-r--r--  1 apple  staff  1600 Aug 14 18:59 test.jar
```

## Execute the code from Pyspark

- Below is our code.

```Python
import os
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# Setting driver path
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/apple/TEST/java/test.jar pyspark-shell'

# Create Spark Session
spark = SparkSession.builder.appName('Test Java Function Call').enableHiveSupport().getOrCreate()

sc = spark.sparkContext

java_import(sc._gateway.jvm, "com.sanmuk.pyjava.Main")
func = sc._gateway.jvm.Main()

# Example of a function with no return type
print("Usage 1: Call function with no return type.\n")
print("Divide {0} by {1}:".format(10, 1))
try:
    func.divNumbers(10, 1)
except Exception as e:
    print("Error occured in division.")
    print(e.message)

# Example of a Java function throwing exception handled in Pyspark
print("Usage 2: Call function and handle exception.\n")
print("Divide {0} by {1}:".format(10, 0))
try:
    func.divNumbers(10, 0)
except Exception as e:
    print("Error occured in division.")
    print(e.__class__)
    print(e.args)

# Example of a Java function returning value processed in Pyspark
print("Usage 3: Call function and accept return.\n")
print("Add {0} with {1}:".format(10, 5))
try:
    s = func.addNumbers(10, 5)
    print("Sum: {}".format(s))
except Exception as e:
    print("Error occured in addition.")
    print(e.__class__)
    print(e.args)
```

- Output to expect

```
Usage 1: Call function with no return type.

Divide 10 by 1:
10

Usage 2: Call function and handle exception.

Divide 10 by 0:
Error occured in division.
<class 'py4j.protocol.Py4JJavaError'>
('An error occurred while calling o32.divNumbers.\n', JavaObject id=o33)

Usage 3: Call function and accept return.

Add 10 with 5:
Sum: 15

Process finished with exit code 0
```
