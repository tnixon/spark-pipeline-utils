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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 9/10/16
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
	* A [[UnarySparkTransformerWrapper]] that expands (explodes) the rows of a dataset
	*
	* @param uid Unique ID for this transformer
	* @tparam IN  Input data type
	* @tparam OUT Output data type
	* @tparam T   This transformer type
	*/
abstract class ExplodingDataSetTransformer[IN, OUT]( uid: String )
	extends UnarySparkTransformerWrapper[IN, Seq[OUT]]( uid )
{
	protected def outputElementType: DataType

	override protected def outputDataType: DataType = ArrayType.apply( outputElementType )

	override def transformSchema( schema: StructType ): StructType =
	{
		val inputType = schema( $( inputCol ) ).dataType
		validateInputType( inputType )
		if (schema.fieldNames.contains( $( outputCol ) )) {
			throw new IllegalArgumentException( s"Output column ${$( outputCol )} already exists." )
		}
		val outputFields = schema.fields :+
			StructField( $( outputCol ), outputElementType, nullable = false )
		StructType( outputFields )
	}

	override def transform( dataset: Dataset[_] ): DataFrame =
	{
		transformSchema( dataset.schema, logging = true )
		val transformUDF = udf( this.createTransformFunc, outputDataType )
		dataset.withColumn( $( outputCol ), explode( transformUDF( dataset( $( inputCol ) ) ) ) )
	}
}

/**
	* Factory methods to construct exploding dataset transformers
	*/
object ExplodingDataSetTransformer
{
	def apply[IN, OUT]
	( uid: String,
	  elementType: DataType,
	  createTx: () => GenericTransformer[IN, Seq[OUT]] ): ExplodingDataSetTransformer[IN, OUT] =
		new ExplodingDataSetTransformer[IN, OUT]( uid )
		{
			override protected def outputElementType: DataType = elementType

			override protected def createTransformer: GenericTransformer[IN, Seq[OUT]] = createTx()
		}

	def apply[IN, OUT]
	( elementType: DataType,
	  createTx: () => GenericTransformer[IN, Seq[OUT]] ): ExplodingDataSetTransformer[IN, OUT] =
		apply[IN, OUT]( classOf[ExplodingDataSetTransformer[IN, OUT]].getName + " of function " + createTx,
		                elementType,
		                createTx )

}