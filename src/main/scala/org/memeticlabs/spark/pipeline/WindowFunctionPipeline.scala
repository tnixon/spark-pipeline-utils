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
 * Created by Tristan Nixon on 2019-08-22.
 */
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

case class WindowRange( start: Long, end: Long )

class WindowFunctionPipeline( override val uid: String )
	extends Transformer
{
	def this() = this( Identifiable.randomUID("WindowFunctionPipeline" ) )

	override def copy( extra: ParamMap ): Transformer = defaultCopy(extra)

	/** Params */

	/** @group param */
	final val aggrStages = new Param[Seq[AggregationTransformer]]( this,
	                                                               "aggregators",
	                                                               "Mappings binding input and output columns to Aggregators" )
	setDefault( aggrStages, Seq() )

	/** @group getParam */
	final def getAggrStages: Seq[AggregationTransformer] = $( aggrStages )

	/** @group setParam */
	final def setAggrStages( values: Seq[AggregationTransformer] ): this.type =
		set( aggrStages, values )

	/** @group param */
	final val partitionCols = new Param[Option[Seq[String]]]( this,
	                                                          "partitionCols",
	                                                          "Names of columns to use for window partitioning.")
	setDefault( partitionCols, None )

	/** @group getParam */
	final def getPartitionCols: Option[Seq[String]] = $(partitionCols)

	/** @group setParam */
	final def setPartitionCols( cols: Seq[String] ): this.type =
		set( partitionCols, Some(cols) )

	/** @group param */
	final val orderCols = new Param[Option[Seq[String]]]( this,
	                                                      "orderingCols",
	                                                      "Names of columns to use for window ordering." )
	setDefault( orderCols, None )

	/** @group getParam */
	final def getOrderCols: Option[Seq[String]] = $( orderCols )

	/** @group setParam */
	final def setOrderCols( cols: Seq[String] ): this.type =
		set( orderCols, Some( cols ) )

	/** @group param */
	final val rangeBetweenRange = new Param[Option[WindowRange]]( this,
	                                                              "rangeBetweenRange",
	                                                              "Start/End range for the window rangeBetween parameters." )
	setDefault( rangeBetweenRange, None )

	/** @group getParam */
	final def getRangeBetweenRange: Option[WindowRange] = $( rangeBetweenRange )

	/** @group setParam */
	final def setRangeBetweenRange( r: WindowRange ): this.type =
		set( rangeBetweenRange, Some(r) )

	/** @group param */
	final val rowsBetweenRange = new Param[Option[WindowRange]]( this,
	                                                             "rowsBetweenRange",
	                                                             "Start/End range for the window rowsBetween parameters." )
	setDefault( rowsBetweenRange, None )

	/** @group getParam */
	final def getRowsBetweenRange: Option[WindowRange] = $( rowsBetweenRange )

	/** @group setParam */
	final def setRowsBetweenRange( r: WindowRange ): this.type =
		set( rowsBetweenRange, Some(r) )

	/** Transform */

	override def transformSchema( schema: StructType ): StructType =
	{
		// must have at least one aggregation
		require( $( aggrStages ) != null && $( aggrStages ).nonEmpty,
		         "Need to have at least one AggregationTransformer set in aggregators!" )

		// we need at least one windowing parameter
		require( $(partitionCols).isDefined ||
		         $(orderCols).isDefined ||
		         $(rangeBetweenRange).isDefined ||
		         $(rowsBetweenRange).isDefined,
		         s"At least one of the window parameters must be set: " +
		         s"${partitionCols.name}, ${orderCols.name}, ${rangeBetweenRange.name}, ${rowsBetweenRange.name}" )

		// validate the partitioning columns
		if( $(partitionCols).isDefined )
			$(partitionCols).get.foreach( col => require(schema.fieldNames.contains(col),
			                                             s"Schema does not contain the partition column ${col}!") )

		// validate the ordering columns
		if( $(orderCols).isDefined )
			$(orderCols).get.foreach( col => require(schema.fieldNames.contains(col),
			                                         s"Schema does not contain the ordering column ${col}!") )

		// validate each stage and collect the outputs
		val outputCols = $(aggrStages).flatMap( _.transformSchema(schema).fields )

		// make sure we don't clobber any existing columns
		outputCols.foreach( col => require( !schema.contains(col),
		                                    s"Schema already contains column ${col}, it will be clobbered!") )

		// append them to the schema
		StructType( schema.fields ++ outputCols )
	}

	private def applyWindowTransform[T]( setter: T => WindowSpec,
	                                     transformer: (T,WindowSpec) => WindowSpec )
	                                   ( params: Option[T] )
	                                   ( prior: Option[WindowSpec] ): Option[WindowSpec] =
		params match {
			case None => prior
			case Some(p) => prior match {
				case None => Some(setter(p))
				case Some(w) => Some(transformer(p,w))
			}
		}

	private val partitionTransformer: Option[Seq[Column]] => Option[WindowSpec] => Option[WindowSpec] =
		applyWindowTransform( (p: Seq[Column]) => Window.partitionBy(p :_*),
		                      (p: Seq[Column], w: WindowSpec) => w.partitionBy(p :_*) )

	private val orderingTransformer: Option[Seq[Column]] => Option[WindowSpec] => Option[WindowSpec] =
		applyWindowTransform( (o: Seq[Column]) => Window.orderBy(o :_*),
		                      (o: Seq[Column], w: WindowSpec) => w.orderBy(o :_*) )

	private val rangeTransformer: Option[WindowRange] => Option[WindowSpec] => Option[WindowSpec] =
		applyWindowTransform( (r: WindowRange) => Window.rangeBetween( r.start, r.end ),
		                      ( r: WindowRange, w: WindowSpec) => w.rangeBetween( r.start, r.end ) )

	private val rowsTransformer: Option[WindowRange] => Option[WindowSpec] => Option[WindowSpec] =
		applyWindowTransform( (r: WindowRange) => Window.rowsBetween( r.start, r.end ),
		                      ( r: WindowRange, w: WindowSpec) => w.rowsBetween( r.start, r.end ) )

	override def transform( ds: Dataset[_] ): DataFrame =
	{
		// transform the schema
		transformSchema( ds.schema, logging = true )

		// set up the transformers
		val partitionTx: Option[WindowSpec] => Option[WindowSpec] =
			partitionTransformer( $(partitionCols).map( _.map(ds(_)) ) )
		val orderingTxt: Option[WindowSpec] => Option[WindowSpec] =
			orderingTransformer( $(orderCols).map( _.map(ds(_)) ) )
		val rangeTx: Option[WindowSpec] => Option[WindowSpec] =
			rangeTransformer( $(rangeBetweenRange) )
		val rowsTx: Option[WindowSpec] => Option[WindowSpec] =
			rowsTransformer( $(rowsBetweenRange) )

		// assemble the window
		val window = rowsTx(rangeTx(orderingTxt(partitionTx(None))))
			.getOrElse(throw new IllegalStateException("Parameters must define a valid window!"))

		// all the existing columns and new columns aggregated over the window
		val selectCols: Seq[Column] = ds.schema.fieldNames.map( ds(_) ) ++
			$(aggrStages).map( _.toColumn(ds,window) )

		// select all of the above
		ds.select( selectCols :_* )
	}
}

object WindowFunctionPipeline
{
	def apply( typeName: String,
	           aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String],
	           orderCols: Seq[String],
	           rangeStart: Long,
	           rangeEnd: Long ): WindowFunctionPipeline =
		new WindowFunctionPipeline( Identifiable.randomUID(typeName) )
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)
			.setOrderCols(orderCols)
			.setRangeBetweenRange( WindowRange( rangeStart, rangeEnd ) )

	def apply( aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String],
	           orderCols: Seq[String],
	           rangeStart: Long,
	           rangeEnd: Long ): WindowFunctionPipeline =
		new WindowFunctionPipeline()
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)
			.setOrderCols(orderCols)
			.setRangeBetweenRange( WindowRange( rangeStart, rangeEnd ) )

	def apply( typeName: String,
	           aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String],
	           orderCols: Seq[String] ): WindowFunctionPipeline =
		new WindowFunctionPipeline( Identifiable.randomUID(typeName) )
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)
			.setOrderCols(orderCols)

	def apply( aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String],
	           orderCols: Seq[String] ): WindowFunctionPipeline =
		new WindowFunctionPipeline()
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)
			.setOrderCols(orderCols)

	def apply( typeName: String,
	           aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String] ): WindowFunctionPipeline =
		new WindowFunctionPipeline( Identifiable.randomUID(typeName) )
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)

	def apply( aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String] ): WindowFunctionPipeline =
		new WindowFunctionPipeline()
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)

	def apply( typeName: String,
	           aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String],
	           rowsStart: Long,
	           rowsEnd: Long  ): WindowFunctionPipeline =
		new WindowFunctionPipeline( Identifiable.randomUID(typeName) )
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)
			.setRowsBetweenRange( WindowRange( rowsStart, rowsEnd ) )

	def apply( aggregations: Seq[AggregationTransformer],
	           partitionCols: Seq[String],
	           rowsStart: Long,
	           rowsEnd: Long  ): WindowFunctionPipeline =
		new WindowFunctionPipeline()
			.setAggrStages(aggregations)
			.setPartitionCols(partitionCols)
			.setRowsBetweenRange( WindowRange( rowsStart, rowsEnd ) )

	def apply( typeName: String,
	           aggregations: Seq[AggregationTransformer],
	           rowsStart: Long,
	           rowsEnd: Long  ): WindowFunctionPipeline =
		new WindowFunctionPipeline( Identifiable.randomUID(typeName) )
			.setAggrStages(aggregations)
			.setRowsBetweenRange( WindowRange( rowsStart, rowsEnd ) )

	def apply( aggregations: Seq[AggregationTransformer],
	           rowsStart: Long,
	           rowsEnd: Long  ): WindowFunctionPipeline =
		new WindowFunctionPipeline()
			.setAggrStages(aggregations)
			.setRowsBetweenRange( WindowRange( rowsStart, rowsEnd ) )
}