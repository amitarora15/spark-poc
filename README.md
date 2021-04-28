Spark is used for multi-processing of big data problems. Spark is working on concept of RDD (Resilient Distributed dataset).
RDD is used for loading the data in distributed way on which Transformations and Actions can be performed. Following are present in this code base

Check for input and output path in Util.java interface

## RDD
* Common Operations
    * Transformation
        * Filter - filtering of data set
        * Map - Getting transformed output from input set
        * FlatMap - Getting multiple output from one input
        * Union & Intersection - set operations
    * Actions
        * count, countByValue, take, reduce
* Pair RDD
    * Creation from Tuple list or from normal RDD by PairMap function  
    * Filter and Map on Pair RDD (mapValues, mapToPair)
    * Reduce By Key & Sort By Key
    * Join operations
* Common Operations
    * Accumulator
    * Broadcast    

    
## Spark SQL
* DataFrame 
    * GroupBy
    * Select
    * Filter
    * Order by
* Dataset
    * Filter
    * Group By    
       