# scala-spark-analytics
How to compile and run a 'Spark Analytics with Movie Data' 

On Iceberg *only* activate a recent version of the git version control software:

```
module load apps/gcc/5.2/git/2.5
```

Get the example
```
git clone https://github.com/casaoffice/teletracking-training
```

Enter the project directory

```
cd scala-spark-HelloWorld/
```

Load the module files for sbt and spark.  On Iceberg do:

```
module load apps/java/1.8u71
module load apps/binapps/sbt/0.13.13 
module load apps/gcc/4.4.7/spark/2.0
```

In ShARC do:

```
module load apps/java/jdk1.8.0_102/binary
module load dev/sbt/0.13.13
module load apps/spark/2.1.0/gcc-4.8.5
```

Compile with 

```
sbt package
```

If this is successful, you'll have a file in the location "/home/ojuarezwork412/sbt_projects/spark-analytics/target/scala-2.11/spark-analytics_2.11-1.0.jar"

.

Run with

```
spark-submit --master local[1] target/scala-2.11/spark-analytics_2.11-1.0.jar
```
