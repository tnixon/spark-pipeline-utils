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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 10/10/16.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
	* A simple type for organizing aggregators and their corresponding output types
	*/
case class AggregatorMapping(output: StructField, aggregator: Column)

/**
	* A Pipeline component that enables a sequence of AggregationTransformers
	*
	* @param uid
	*/
class AggregationPipeline(override val uid: String) extends Transformer
{
	def this() = this( Identifiable.randomUID( "AggregationPipeline" ) )

	override def copy(extra: ParamMap): Transformer = defaultCopy( extra )

	/** Params */

	/** @group param */
	final val groupCols = new Param[Seq[String]]( this, "groupCols", "Columns to group by" )
	setDefault( groupCols, Seq() )
	/** @group param */
	final val aggrStages = new Param[Seq[AggregationTransformer]]( this,
	                                                               "aggregators",
	                                                               "Mappings binding input and output columns to Aggregators" )
	setDefault( aggrStages, Seq() )

	/** @group getParam */
	final def getGroupCols = $( groupCols )

	/** @group getParam */
	final def getAggrStages = $( aggrStages )

	/** @group setParam */
	final def setGroupCols(values: Seq[String]) = set( groupCols, values )

	/** @group setParam */
	final def setAggrStages(values: Seq[AggregationTransformer]): AggregationPipeline =
		set( aggrStages, values )

	/** Transform */

	override def transformSchema(schema: StructType): StructType =
	{
		// validate the parameters
		require( $( groupCols ) != null && $( groupCols ).size > 0, "groupCols must be set!" )
		require( $( aggrStages ) != null && $( aggrStages ).size > 0, "measureCols must be set!" )

		// validate the schema
		$( groupCols ).foreach( col => require( schema.fieldNames.contains( col ),
		                                        s"${uid}: Grouping column ${col} does not exist!" ) )

		// start with the grouping columns
		val grpCols = schema.fields.filter( col => $( groupCols ).contains( col.name ) )
		// validate each stage, and collect their output columns
		val outputCols = $( aggrStages ).flatMap( _.transformSchema( schema ).fields )
		// append them to the group columns
		StructType( grpCols ++ outputCols )
	}

	override def transform(ds: Dataset[_]): DataFrame =
	{
		// transform the schema
		transformSchema( ds.schema, logging = true )
		// group the dataset by the grouping columns
		val gds = ds.groupBy( $( groupCols ).map( gc => ds( gc ) ): _* )
		// bind columns to input, output columns
		val aggCols = $( aggrStages ).map( _.toColumn( ds ) )
		// and apply the aggregations
		aggCols.size match {
			case 1 => gds.agg( aggCols( 0 ) )
			case _ => gds.agg( aggCols.head, aggCols.tail: _* )
		}
	}
}

/**
	* Factory methods for building AggregationPipelines
	*/
object AggregationPipeline
{
	def asPipeline(groupBy: Seq[String], aggregators: Seq[AggregationTransformer]): AggregationPipeline =
		new AggregationPipeline().setGroupCols( groupBy ).setAggrStages( aggregators )

	def asPipeline(groupBy: String, aggregators: Seq[AggregationTransformer]): AggregationPipeline =
		asPipeline( Seq( groupBy ), aggregators )

	def asPipeline(groupBy: Seq[String], aggregator: AggregationTransformer): AggregationPipeline =
		asPipeline( groupBy, Seq( aggregator ) )

	def asPipeline(groupBy: String, aggregator: AggregationTransformer): AggregationPipeline =
		asPipeline( Seq( groupBy ), Seq( aggregator ) )
}
