# Execute Java Method

Sometimes there are requirement for calling Java methods from Pyspark driver program (Note here we are not talking about UDF which execute in Executors). Sometime some implementations are natural with Java also there are ready made third party implentation already available in Java. Lets see how we use Java code from Pyspark driver.

## Create a demo Java function

Our sample Java function.

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

Lets compile and create a Jar.


