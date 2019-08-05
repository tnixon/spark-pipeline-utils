/**
	* Copyright 2019 Tristan Nixon
	*
	* Licensed under the Apache License, Version 2.0 (the "License");
	* you may not use this file except in compliance with the License.
	* You may obtain a copy of the License at
	*
	* http://www.apache.org/licenses/LICENSE-2.0
	*
	* Unless required by applicable law or agreed to in writing, software
	* distributed under the License is distributed on an "AS IS" BASIS,
	* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	* See the License for the specific language governing permissions and
	* limitations under the License.
	*
	* Created by Tristan Nixon on 2019-08-02.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class MultiInputTransformerWrapper[OUT,T <: MultiInputTransformerWrapper[OUT, T]]( override val uid: String)
	extends Transformer with HasOutputCol
{
	override def copy( extra: ParamMap ): Transformer = defaultCopy(extra)

	/** Params */

	/** @group param */
	final val inputCols = new Param[Seq[String]]( this,
	                                              "inputCols",
	                                              "The list of input columns for this function" )
	setDefault( inputCols, Seq[String]() )

	/** @group getParam */
	final def getInputCols: Seq[String] = $(inputCols)

	/** @group setParam */
	final def setInputCols( values: Seq[String] ): T = set(inputCols,values).asInstanceOf[T]

	final def addInputCol( value: String ): T =
		set( inputCols, $(inputCols) :+ value ).asInstanceOf[T]

	/** @group setParam */
	final def setOutput( value: String ): T = set(outputCol,value).asInstanceOf[T]

	/**
		* Creates the transform function using the given param map. The input param map already takes
		* account of the embedded param map. So the param values should be determined solely by the input
		* param map.
		*/
	protected def transformFunc[IN]: IN => OUT

	/**
		* Returns the data types of the input parameters
		*/
	protected def inputDataTypes: Seq[DataType]

	/**
		* Returns the data type of the output column.
		*/
	protected def outputDataType: DataType

	/** Transform */

	override def transformSchema( schema: StructType ): StructType =
	{
		// make sure we have all the input columns
		$(inputCols).zip(inputDataTypes).foreach( input =>
			                                          schema.find( _.name.equals(input._1) ) match
		{
			case Some(column) => require( column.dataType.equals(input._2),
			                              s"Expected input column ${column.name} to have type ${input._2}, " +
				                              s"found ${column.dataType} instead!" )
			case None => throw new IllegalArgumentException(s"No column named ${input._1} found in schema!")
		})

		// should not have output column
		if (schema.fieldNames.contains($(outputCol)))
			throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")

		// add the output column to the schema
		val outputFields = schema.fields :+
			StructField($(outputCol), outputDataType, nullable = false)
		StructType(outputFields)
	}

	override def transform( ds: Dataset[_] ): DataFrame =
	{
		transformSchema(ds.schema, logging = true)

		// create a UDF
		val txUDF = UserDefinedFunction( transformFunc,
		                                 outputDataType,
		                                 Some(inputDataTypes) )

		// apply the UDF to the columns
		ds.withColumn( $(outputCol), txUDF( $(inputCols).map( col ) :_* ) )
	}
}