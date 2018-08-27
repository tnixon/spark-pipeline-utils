# spark-pipeline-utils
Utility classes that extend and generalize Spark's ML pipeline framework. 
In particular, it adds the following features:

* support for Transformer-only pipelines (i.e. ETL pipelines)
* support for aggregations as pipeline stages, and multi-aggregation pipelines
* support for "exploding" transformers (ie. using the explode function to expand rows in a DataFrame)
* a few handy transformers that expand upon what's already provided in the Spark ML API, including:
    * Column selection, dropping and renaming

### Why do we need this?

SQL code is messy and quickly gets hard to manage and interpret. 
Regardless of whether or not you're training a machine learning model,
organizing your code into a modular, configurable framework makes it much 
easier to manage, generalize and share. These are all good things.

The existing Spark ML Pipelines API does a great job of providing a modular, configurable and
extensible framework for packaging up Spark SQL operations into reusable components (Transformers),
and chaining them together into pipelines.

**However**, the API as it is assumes that all pipelines are *Estimators*, that is, that you would
only ever be training an Estimator model or using a trained model to produce estimations. This 
leaves a lot of potentially productive uses of the pipelines API out of reach. You cannot, 
for example, use this framework to:

* Build an ETL pipeline
* Modularize the data-prep stages of your model training into re-usable pipelines

It is also not easy to build Transformers that alter the number of rows in a dataset, for example, to:

* explode a single row into multiple rows (i.e. by segmenting a field)
* aggregate across multiple rows

In order to make the ML Pipelines more flexible, I've created a number of utility classes that 
address the above gaps. These are fully compatile with the existing Spark ML Pipelines APIs, and
can be combined with any other Transformers or Estimators out there. Hopefully, 
these will prove to be useful to others (I've found them very helpful). 

### TODO: describe classes

### TODO: provide examples