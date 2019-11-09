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
package org.memeticlabs.spark.ml.utils.pipelines

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
	* A Pipeline component that enables a sequence of AggregationTransformers
	*
	* @param uid unique ID of this Transformer
	*/
class AggregationTransformerPipeline( override val uid: String ) extends Transformer
{
	def this() = this( Identifiable.randomUID( "AggregationPipeline" ) )

	override def copy( extra: ParamMap ): Transformer = defaultCopy( extra )

	/** Params */

	/** @group param */
	final val groupCols = new Param[Seq[String]]( this, "groupCols", "Columns to group by" )
	setDefault( groupCols, Seq() )

	/** @group param */
	final val aggrStages = new Param[Seq[AggregationStage]]( this,
	                                                         "aggregators",
	                                                         "Mappings binding input and output columns to Aggregators" )
	setDefault( aggrStages, Seq() )

	/** @group getParam */
	final def getGroupCols: Seq[String] = $( groupCols )

	/** @group getParam */
	final def getAggrStages: Seq[AggregationStage] = $( aggrStages )

	/** @group setParam */
	final def setGroupCols( values: Seq[String] ): AggregationTransformerPipeline.this.type = set( groupCols, values )

	/** @group setParam */
	final def setAggrStages( values: Seq[AggregationStage] ): AggregationTransformerPipeline.this.type =
		set( aggrStages, values )

	/** Transform */

	override def transformSchema( schema: StructType ): StructType =
	{
		// validate the parameters
		require( $( groupCols ) != null && $( groupCols ).nonEmpty, "groupCols must be set!" )
		require( $( aggrStages ) != null && $( aggrStages ).nonEmpty,
		         "Need to have at least one AggregationTransformer set in aggregators!" )

		// validate the schema
		$( groupCols ).foreach( col => require( schema.fieldNames.contains( col ),
		                                        s"$uid: Grouping column $col does not exist!" ) )

		// start with the grouping columns
		val grpCols = schema.fields.filter( col => $( groupCols ).contains( col.name ) )
		// validate each stage, and collect their output columns
		val outputCols = $( aggrStages ).flatMap( _.transformSchema( schema ).fields )
		// append them to the group columns
		StructType( grpCols ++ outputCols )
	}

	override def transform( ds: Dataset[_] ): DataFrame =
	{
		// transform the schema
		transformSchema( ds.schema, logging = true )
		// group the dataset by the grouping columns
		val gds = ds.groupBy( $( groupCols ).map( gc => ds( gc ) ): _* )
		// bind columns to input, output columns
		val aggCols = $( aggrStages ).map( _.toColumn( ds ) )
		// and apply the aggregations
		aggCols.size match {
			case 1 => gds.agg( aggCols.head )
			case _ => gds.agg( aggCols.head, aggCols.tail: _* )
		}
	}
}

/**
	* Factory methods for building AggregationPipelines
	*/
object AggregationTransformerPipeline
{
	def apply( groupBy: Seq[String], aggregators: AggregationStage* ): AggregationTransformerPipeline =
		new AggregationTransformerPipeline().setGroupCols( groupBy ).setAggrStages( aggregators )

	def apply( groupBy: String, aggregators: AggregationStage* ): AggregationTransformerPipeline =
		new AggregationTransformerPipeline().setGroupCols( Seq( groupBy ) ).setAggrStages( aggregators )
}
