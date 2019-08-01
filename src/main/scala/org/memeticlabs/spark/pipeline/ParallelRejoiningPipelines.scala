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
	* Created by Tristan Nixon on 2019-07-29.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
	* A class for staging multiple Transformers in parallel
	* that are finally joined together, like so:
	*
	*             __Transformer1__
	*            /                \
	* initial DF ---Transformer2--- Join-- final DF
	*           \                 /
	*            ---Transformer3--
	*
	* When this transformer is applied to a DataFrame,
	* it applies each of N different transformers to the DataFrame
	* independently, and then joins them together to produce a final
	* resulting dataframe with the results of each transformation.
	*
	* By default, it also caches the initial data frame for performance reasons.
	*/
class ParallelRejoiningPipelines(override val uid: String)
	extends Transformer
{
	def this() = this( Identifiable.randomUID("ParallelRejoiningPipelines" ) )

	override def copy( extra: ParamMap ): Transformer = defaultCopy(extra)

	/** Params */

	/** @group param */
	final val joinCols = new Param[Seq[String]]( this,
	                                             "joinCols",
	                                             "Columns used to re-join the resulting DataFrames. " +
		                                             "These must exist in all DataFrames. Leaving this parameter unset " +
		                                             "will result in all common columns being used in the join.")
	setDefault(joinCols, Seq())

	/** @group getParam */
	final def getJoinCols: Seq[String] = $(joinCols)

	/** @group setParam */
	final def setJoinCols( cols: Seq[String] ): ParallelRejoiningPipelines.this.type = set( joinCols, cols )

	/** @group param */
	final val parallelTransformers = new Param[Seq[Transformer]]( this,
	                                                              "parallelTransformers",
	                                                              "Transformers applied in parallel to a DataFrame, " +
		                                                              "and then re-joined to create the output.")
	setDefault(parallelTransformers, Seq())

	/** @group getParam */
	final def getParallelTransformers: Seq[Transformer] = $(parallelTransformers)

	/** @group setParam */
	final def setParallelTransformers( tx: Seq[Transformer] ): ParallelRejoiningPipelines.this.type =
		set( parallelTransformers, tx )

	/** Transform */

	private def hasJoinCols: Boolean =
		$(joinCols) != null && $(joinCols).nonEmpty

	override def transformSchema( schema: StructType ): StructType =
	{
		// a type to track named schemas
		case class NamedSchema( txName: String, schema: StructType )

		// make sure each transformer can transform this schema
		val parallelSchemas =
			$(parallelTransformers).map( tx => NamedSchema( tx.uid , tx.transformSchema(schema) ) )

		// curried function to merge our named schemas
		def schemaJoiner( validator: (NamedSchema, NamedSchema) => Unit )
		                ( a: NamedSchema, b: NamedSchema ): NamedSchema =
		{
			// validate the join
			validator( a, b )

			// join the schemas
			NamedSchema( "( "+ a.txName +" + "+ b.txName +" )",
			             StructType( a.schema.union(b.schema).toArray ) )
		}

		// function to validate our
		val joinValidator: (NamedSchema, NamedSchema) => Unit =
			if( hasJoinCols )
				( a: NamedSchema, b: NamedSchema) => {
					// make sure both tables have the join columnns
					val aColnames = a.schema.fieldNames
					val bColnames = b.schema.fieldNames
					$( joinCols ).foreach( col => {
						require( aColnames.contains( col ),
						         s"Parallel transform results ${a.txName} don't all contain the join column ${col}" )
						require( bColnames.contains( col ),
						         s"Parallel transform results ${b.txName} don't all contain the join column ${col}" )
					} )
					// make sure they don't clobber any other columns
					val clobberCols = aColnames.diff($(joinCols))
					                           .intersect(bColnames.diff($(joinCols)))
					require( clobberCols.isEmpty,
					         s"Parallel transform results ${a.txName} and ${b.txName} will clobber non-join columns ${clobberCols}" )
				}
			else
				( a: NamedSchema, b: NamedSchema) =>
					// make sure that we have common columns to join on
					require( a.schema.intersect(b.schema).nonEmpty,
					         s"Parallel transform results ${a.txName} and ${b.txName} have no columns in common" )

		// apply the validation function over the tables
		parallelSchemas.reduceLeft( schemaJoiner(joinValidator) ).schema
	}

	override def transform( ds: Dataset[_] ): DataFrame =
	{
		// transform the schema
		transformSchema( ds.schema, logging = true )

		// apply each transformer to the source
		val parallelDFs = $(parallelTransformers).map( _.transform(ds) )

		// figure out how we're going to join our dataframes
		val joinFn: (DataFrame,DataFrame) => DataFrame =
			if( hasJoinCols )
				(a: DataFrame, b: DataFrame) => a.join(b, $(joinCols))
			else
				(a: DataFrame, b: DataFrame) => a.join(b)

		// join everything back together
		parallelDFs.reduceLeft( joinFn )
	}
}
