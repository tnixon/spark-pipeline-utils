# spark-pipeline-utils
Utility classes that extend and generalize Spark's ML pipeline framework. 
In particular, it adds the following features:

* support for Transformer-only pipelines (i.e. ETL pipelines)
* support for aggregations as pipeline stages, and multi-aggregation pipelines
    * Use any Spark aggregation function
    * Use your own User-Defined Aggregation functions (UDAFs)
* support for windowing functions as pipeline stages, and pipelines of multiple windowing functions
* support for "exploding" transformers (ie. using the explode function to expand rows in a DataFrame)
* support for running multiple pipelines in parallel, and then re-joining their results based on common columns
    * especially useful if you need to a source DataFrame up to several different levels of granularity
* a few handy transformers that expand upon what's already provided in the Spark ML API, including:
    * Column selection, dropping and renaming
    * Wrap any function into a tranformer stage (current ML framework only provides a 1-to-1 (ie. Unary) transformer)

### Why do we need this?

SQL code is messy and quickly gets hard to manage, debug and interpret. 
Regardless of whether or not you're training a machine learning model,
organizing your code into a modular, configurable framework makes it much 
easier to manage, generalize and share. These are all good things, especially 
for production-quality codebases.

The existing Spark ML Pipelines API does a great job of providing a modular, configurable and
extensible framework for packaging up Spark SQL operations into reusable components (Transformers),
and chaining them together into pipelines.

**However**, the API as it is assumes that all pipelines are *Estimators*, that is, that you would
only ever be training an Estimator model or using a trained model to produce estimations. This 
leaves a lot of potentially productive uses of the pipelines API out of reach. You cannot, 
for example, use this framework to:

* Build an ETL pipeline
* Modularize the data-prep stages of your model training into re-usable pipelines
* Tune the hyper-parameters of complex data-preparation stages that feed into ML models

It is also not easy to build Transformers that alter the number of rows in a dataset, for example, to:

* explode a single row into multiple rows (i.e. by segmenting a field)
* aggregate across multiple rows

Even windowing functions, which don't actually change the numbers of rows, 
but compute functions across groups of rows, are hard or impossible to implement.

In order to make the ML Pipelines more flexible, I've created a number of utility classes that 
address the above gaps. These are fully compatile with the existing Spark ML Pipelines APIs, and
can be combined with any other Transformers or Estimators out there. Hopefully, 
these will prove to be useful to others (I've found them very helpful in my own projects). 

### Transformer Utility Classes

#### FunctionTransformerWrapper

This class allows you to wrap any N-to-1 function as a transformer stage.

0-argument functions:
```scala
import java.util.UUID
import org.memeticlabs.spark.ml.utils.transformers.FunctionTransformerWrapper

val idGenerator = FunctionTransformerWrapper( "IDGenerator", "idColumnName", UUID.randomUUID )
```

1-argument functions:
```scala
import org.memeticlabs.spark.ml.utils.transformers.FunctionTransformerWrapper

val cosineTransformer = FunctionTransformerWrapper[Double,Double]( "CosineTransformer", 
	                                                               "inputColumName", 
	                                                               "outputColumnName", 
	                                                               scala.math.cos )
```

Multiple argument functions:
```scala
import scala.math.abs
import org.memeticlabs.spark.ml.utils.transformers.FunctionTransformerWrapper

case class Point( x: Double, y: Double )

val manhattanDistance = FunctionTransformerWrapper( "ManhattanDistance",
                                                    Seq("XColumnName","YColumnName"),
                                                    "manhattanDistanceColumnName",
                                                    ( a: Point, b: Point ) =>
	                                                    abs( a.x - b.x ) + abs( a.y - b.y ) )
```

#### ColumnSelector

This transformer allows you to select out only specific columns from a **DataFrame**.

```scala
import org.memeticlabs.spark.ml.utils.transformers.ColumnSelector

val selector = ColumnSelector( "ColumnA", "ColumnB" )
```

#### ColumnDropper

This transformer allows you to drop only specific columns from a **DataFrame**.

```scala
import org.memeticlabs.spark.ml.utils.transformers.ColumnDropper

val dropper = ColumnDropper( "ColumnA", "ColumnB" )
```

#### ColumnRenamer

This transformer allows you to rename column names.

```scala
import org.memeticlabs.spark.ml.utils.transformers.ColumnRenamer

val renamer = ColumnRenamer( "A" -> "X", "B" -> "Y" )
```

### Pipeline Utility Classes

#### TransformerPipeline

A sequence of **Transformers** in a **Pipeline** that is itself a **Transformer**, not an **Estimator**. 
This allows you to group multiple Transformers together in sequence, and run them on their own
as an ETL pipeline, or include them as the data-preparation stages of an Estimator Pipeline.

```scala
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.memeticlabs.spark.ml.utils.pipelines.TransformerPipeline

val df: DataFrame = ???
val stage1: Transformer = ???
val stage2: Transformer = ???
val stage3: Transformer = ???
val stage4: Transformer = ???

val transformerPipeline = TransformerPipeline( stage1, stage2, stage3, stage4 )

val transformed = transformerPipeline.transform( df )
```

#### ExplodingDataSetTransformer

A **Transformer** that "explodes" a column, by splitting values into multiple parts, 
and producing a new row for each part (where all other columns have their values replicated).

```scala
import org.apache.spark.sql.types.StringType
import org.memeticlabs.spark.ml.utils.transformers.ExplodingDataSetTransformer

// a simple tokenizer that splits based on whitespace
val simpleTokenizer = ExplodingDataSetTransformer( "SimpleTokenizer",
	                                               StringType,
	                                               "TextColumnName",
	                                               "TokenColumnName",
	                                               (text: String) => text.split("\\w+" ) )
``` 

#### AggregationStage

An **AggregationStage** consists of an aggregation function over one or more columns.
These may be added to an **AggregationTransformerPipeline** to aggregate 
a **DataFrame** by a common set of factors. They may also be added to a **WindowingTransformerPipeline**
to aggregate within a defined **Window**. Details on each case follow.

#### AggregationTransformerPipeline

Add one or more **AggregationStage**s to an **AggregationTransformerPipeline** to compute multiple
aggregations over a **DataFrame** by a common set of factors. 

```scala
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{avg, variance}
import org.apache.spark.sql.types.DoubleType

import org.memeticlabs.spark.ml.utils.pipelines.AggregationStage
import org.memeticlabs.spark.ml.utils.pipelines.AggregationTransformerPipeline

val meanStage = AggregationStage( "inputColName", "meanColName", DoubleType, (vals: Column) => avg(vals) )
val varStage = AggregationStage( "inputColName", "varColName", DoubleType, (vals: Column) => variance(vals) )

val meanVarianceTransformer = AggregationTransformerPipeline( "idColName", meanStage, varStage )
```

#### WindowingTransformerPipeline

Add one or more **AggregationStage**s to a **WindowingTransformerPipeline** to compute
aggregations within a **Window** over a **DataFrame**.

```scala
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{avg, variance}
import org.apache.spark.sql.types.DoubleType

import org.memeticlabs.spark.ml.utils.pipelines.AggregationStage
import org.memeticlabs.spark.ml.utils.pipelines.WindowingTransformerPipeline

// compute a rolling average for each ID, of the past 10 values, ordered by timestamps
val meanStage = AggregationStage( "inputColName", "meanColName", DoubleType, (vals: Column) => avg(vals) )
val varStage = AggregationStage( "inputColName", "varColName", DoubleType, (vals: Column) => variance(vals) )

val meanVarianceTransformer = WindowingTransformerPipeline( Seq( meanStage, varStage ),
                                                            Seq( "idColName" ),
                                                            Seq( "timestampColName" ),
                                                            -10, 0 )
```

#### ParallelRejoiningPipelines

Run several pipelines in parallel, and join their results on common columns. This can be
especially useful when you need to aggregate a **DataFrame** up to multiple levels.

```scala
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{avg, variance}
import org.apache.spark.sql.types.DoubleType

import org.memeticlabs.spark.ml.utils.pipelines.AggregationStage
import org.memeticlabs.spark.ml.utils.pipelines.AggregationTransformerPipeline
import org.memeticlabs.spark.ml.utils.pipelines.ParallelRejoiningPipelines

val meanStage = AggregationStage( "inputColName", "meanColName", DoubleType, (vals: Column) => avg(vals) )
val varStage = AggregationStage( "inputColName", "varColName", DoubleType, (vals: Column) => variance(vals) )

val idAggr = AggregationTransformerPipeline( "idColName", meanStage, varStage )
val groupAggr = AggregationTransformerPipeline( Seq("idColName", "groupColName"), meanStage, varStage )

val rollup = ParallelRejoiningPipelines( Seq( "idColName" ),
                                         Seq( idAggr, groupAggr ),
                                         "outer" )
```
