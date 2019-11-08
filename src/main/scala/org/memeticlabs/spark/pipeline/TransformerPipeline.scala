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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 9/5/16.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
	* A generalization of spark ML pipelines to include
	* Transformer-only pipelines (ie. a Pipeline that
	* acts like a Transformer, not an Estimator)
	*/
class TransformerPipeline( override val uid: String )
	extends Transformer
{
	def this( ) = this( Identifiable.randomUID( "TransformerPipeline" ) )

	/** @group param */
	val stages: Param[Array[Transformer]] = new Param( this, "stages", "stages of the pipeline" )

	/** @group setParam */
	def setStages( value: Array[Transformer] ): TransformerPipeline.this.type =
		set( stages, value.asInstanceOf[Array[Transformer]] )

	/** @group getParam */
	def getStages: Array[Transformer] = $( stages ).clone()

	override def copy( extra: ParamMap ): TransformerPipeline =
	{
		val map = extractParamMap( extra )
		val newStages = map( stages ).map( _.copy( extra ) )
		new TransformerPipeline().setStages( newStages )
	}

	def schemaAfter( stage: Int, startingWithSchema: StructType ): StructType =
	{
		// check the
		require( stage > 0, s"stage must be a positive number, got $stage instead")
		require( stage <= $(stages).size,
		         s"stage must be no greater than the number of ${getStages.size}, but got $stage instead")
		// compute the schema after the stage-th stage
		$(stages).dropRight($(stages).size - stage)
		         .foldLeft(startingWithSchema)( (schema: StructType, tx: Transformer) => tx.transformSchema(schema) )
	}

	def schemaAfter( stage: Int, startingWith: Dataset[_] ): StructType =
		schemaAfter(stage, startingWith.schema)

	override def transformSchema( schema: StructType ): StructType =
	{
		val theStages = $( stages )
		require( theStages.toSet.size == theStages.length, s"$uid: Cannot have duplicate components in a pipeline." )
		theStages.foldLeft( schema )( ( cur, stage ) => stage.transformSchema( cur ) )
	}

	override def transform( dataset: Dataset[_] ): DataFrame =
	{
		transformSchema( dataset.schema, logging = true )
		$( stages ).foldLeft( dataset.toDF )( ( cur, transformer ) => transformer.transform( cur ) )
	}
}

object TransformerPipeline
{
	def apply( transformers: Array[Transformer] ): TransformerPipeline =
		new TransformerPipeline().setStages( transformers )
}
