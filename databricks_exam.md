Correct Answers
1. B
2. E
3. E
4. D
5. E
6. A
7. A
8. E
9. E
10. D
11. A
12. B
13. A
14. A
15. D
16. D
17. C
18. C
19. A
20. B
21. A
22. A
23. D
24. C
25. B
26. C
27. A
28. D
29. E
30. E
31. A
32. A
33. D
34. E
35. B
36. A
37. E
38. B
39. D
40. D
41. B
42. A
43. C
44. B
45. D
46. D
47. C
48. A
49. B
50. B
51. B
52. A
53. E
54. A
55. E
56. E
57. E
58. C
59. D
60. E



Slots are resources for parallelization within a Spark application.  Threads available to perform work.

A Spark task is a combination of a block of data and a set of transformations to be ran on a single executor.
    * A job could be composed of many of these tasks.  Hence, the single executor bit.

A Spark Stage is a group of tasks to be executed in parallel to compuse the same set of operations on multiple machines.

If there are more partitions than number of executors, then not all data can be processed at the same time.  There will also be a large number of shuffle connections needed, a lot of overhead with managing processing resources within each task, and there might be out-of-memory errors.

[Which will Trigger Evaluation](https://discord.com/channels/272962334648041474/272962334648041474/980524584203845642) - only the `.count` operation will.

Transformations are business logic operations that do not induce execution, actions only trigger the atual execution to return the results.

[Narrow Transformation](https://discord.com/channels/272962334648041474/272962334648041474/980524997833527376) - only `.select` because all the others involve pretty complex transformation steps.
- ![image](https://user-images.githubusercontent.com/16946556/213941901-2749caeb-d39a-4311-bf09-1ed0e4fab35d.png)
- Narrow Transformations are small transformations that require no shuffling and are using pretty fast, like `map()` and `filter().
- Wide Transformations are slower, typically involves shuffles like `groupBy()` and `join()`.

Spark has 3 execution/deployment modes: cluster, client, and local.  They determine where the driver and executors are physically located when a Spark application is run.

Each worker node has at least 1 executor in it.

Spark cluster configurations will all ensure completion of a Spark application bc worker nodes are fault-tolerant, regardless of how many executors there are.

OOM errors are when the driver or an executor does not have enough memory to collect or process the data allocated to it.

Default storage level for `persist()` for non-streaming DataFrame/Dataset is memory_and_disk.

A broadcast variable is cached on each worker node so it doesn't need to be shipped or shuffled between nodes within each stage.

Spark DataFrames are built on top of RDDs.

`storesDF.select("storeId", "division")` to select 2 columns from a DF.  `storesDF.drop("storeId", "division")` to drop 2 columns from a DF.

`storesDF.filter(col("sqft") <= 25000)` to filter a column called sqft.

`storesDF.filter((col("sqft") <= 25000) | (col("customerSatisfaction") >= 30))`

so when selecting or dropping columns you just need to wrap them in quotes, but for performing operations on them you need to call `col()`

`col().cast()` to cast a column to a different data type.

`storesDF.withColumn("sqft100", col("sqft") / 100)` create a new DF with a new column.

`storesDF.withColumn("numberOfManagers", lit("1"))` to make a column numberOfManagers with a constant int of 1.

[Test](https://files.training.databricks.com/assessments/practice-exams/PracticeExam-DCADAS3-Python.pdf) left off at question 25.

`split()` is an imported functions object, it is not a method of a column object.  use `F.split()`.

`explode()` splits an array column into an individual DF row for each element in an array.

`storesDF.withColumn("storeCategory", lower(col("storeCategory")))` renames the storeCategory column to all lowercase.

`(storesDF.withColumnRenamed("division", "state").withColumnRenamed("managerName", "managerFullName"))` - renames the state column to division and the managerName column into managerFullName.

`storesDF.na.drop("all")` if all values in a record are missing for every column then drop the row.

```
A. DataFrame.distinct()
B. DataFrame.drop_duplicates(subset = None)
C. DataFrame.drop_duplicates()
D. DataFrame.dropDuplicates()
E. DataFrame.drop_duplicates(subset = "all")
```
^ from above, only E. will fail to return a dataframe where every row is unique.

```
A. storesDF.agg(approx_count_distinct(col("division")).alias("divisionDistinct"))
B. storesDF.agg(approx_count_distinct(col("division"), 0).alias("divisionDistinct"))
C. storesDF.agg(countDistinct(col("division")).alias("divisionDistinct"))
D. storesDF.select("division").dropDuplicates().count()
E. storesDF.select("division").distinct().count()
```
^ from above, only A. will not always return the exact number of distinct values in the column division.

`storesDF.count()` returns the number of rows in a dataframe.

`storesDF.groupBy("division").agg(sum(col("sqft")))` will group by division and then aggregate sum of the sqft column.

`storesDF.describe("sqft")` returns summary statistics for only the sqft column in the dataframe.

`sort() and orderBy()` sorts rows of a dataframe.

left off at 37.