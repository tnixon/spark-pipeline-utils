/**
	* Copyright 2016,2017,2018 Tristan Nixon
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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 10/26/16.
	*/
package org.memeticlabs.spark.ml.utils.pipelines

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, WindowSpec}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, Dataset}

/**
	* Framework for doing arbitrary aggregations in a Spark ML Pipeline
	*/
abstract class AggregationStage( override val uid: String ) extends PipelineStage
{
	def this( ) = this( Identifiable.randomUID( "AggregationTransformer" ) )

	/** @group param */
	final val inputCols: Param[Seq[String]] = new Param[Seq[String]]( this,
	                                                                  "inputCols",
	                                                                  "Input Columns to this Aggregation Transformer" )
	/** @group param */
	final val outputCol: Param[String] = new Param[String]( this,
	                                                        "outputCol",
	                                                        "Output Column for this Aggregation Transformer" )

	/**
		* @group getParam
		* @return
		*/
	def getInputCols: Seq[String] = $( inputCols )

	/**
		* @group getParam
		* @return
		*/
	def getOutputcol: String = $( outputCol )

	/**
		* @group setParam
		* @param values
		* @return
		*/
	def setInputCols( values: Seq[String] ): AggregationStage.this.type = set( inputCols, values )

	/**
		* @group setParam
		* @param value
		* @return
		*/
	def setOutputCol( value: String ): AggregationStage.this.type = set( outputCol, value )

	override def copy( extra: ParamMap ): PipelineStage = defaultCopy( extra )

	/**
		* Creates the transform function using the given param map. The input param map already takes
		* account of the embedded param map. So the param values should be determined solely by the input
		* param map.
		*/
	def aggregationFunction: Seq[Column] => Column

	/**
		* Returns the data type of the output column.
		*/
	def outputDataType: DataType

	// since we're aggregating, only the output column will be in the resulting schema
	override def transformSchema( schema: StructType ): StructType =
	{
		// make sure we have our input columns!
		$( inputCols ).foreach( col => require( schema.fieldNames.contains( col ),
		                                        s"$uid: Input column $col does not exist!" ) )
		// make sure the output column is not already defined
		// (or that it's part of the input - so it's getting re-purposed)
		require( !schema.fieldNames.contains( $( outputCol ) ) ||
		         $(inputCols).contains($(outputCol)),
		         s"$uid: Table already contains output column ${$( outputCol )}!" )
		// in an aggregation, we're only going to end up with the specified output columns
		StructType( Seq( StructField( $( outputCol ), outputDataType ) ) )
	}

	def toColumn( ds: Dataset[_] ): Column =
		aggregationFunction( $(inputCols).map( ds(_) ) )
			.cast( outputDataType )
			.as( $(outputCol) )

	def toColumn( ds: Dataset[_], overWindow: WindowSpec ): Column =
		aggregationFunction( $(inputCols).map( ds(_) ) )
			.over(overWindow)
			.cast(outputDataType)
			.as($(outputCol))
}

/**
	* Helper methods to construct Aggregation transformers
	*/
object AggregationStage
{
	/**
		* Generate a transformer for a simple aggregation expression
		*
		* @param inputCols
		* @param outputCol
		* @param outputType
		* @param aggFn
		* @return
		*/
	def apply( inputCols: Seq[String],
	           outputCol: String,
	           outputType: DataType,
	           aggFn: Seq[Column] => Column ): AggregationStage =
		new AggregationStage()
		{
			override def aggregationFunction: Seq[Column] => Column = aggFn

			override def outputDataType: DataType = outputType
		}.setInputCols( inputCols )
			.setOutputCol( outputCol )

	/**
		* Generate a transformer for a simple aggregation expression
		*
		* @param inputCol
		* @param outputCol
		* @param outputType
		* @param aggFn
		* @return
		*/
	def apply( inputCol: String,
	           outputCol: String,
	           outputType: DataType,
	           aggFn: Column => Column ): AggregationStage =
		apply( Seq( inputCol ), outputCol, outputType, ( in: Seq[Column] ) => aggFn( in.head ) )

	/**
		*
		* @param inputCols
		* @param outputCol
		* @param udaf
		* @return
		*/
	def apply( inputCols: Seq[String],
	           outputCol: String,
	           udaf: UserDefinedAggregateFunction ): AggregationStage =
		new AggregationStage( Identifiable.randomUID( udaf.getClass.getSimpleName + "_asTransformer" ) )
		{
			override def aggregationFunction: Seq[Column] => Column = udaf.apply

			override def outputDataType: DataType = udaf.dataType
		}.setInputCols( inputCols )
			.setOutputCol( outputCol )

	/**
		*
		* @param inputCol
		* @param outputCol
		* @param udaf
		* @return
		*/
	def apply( inputCol: String,
	           outputCol: String,
	           udaf: UserDefinedAggregateFunction ): AggregationStage =
		apply( Seq( inputCol ), outputCol, udaf )
}
