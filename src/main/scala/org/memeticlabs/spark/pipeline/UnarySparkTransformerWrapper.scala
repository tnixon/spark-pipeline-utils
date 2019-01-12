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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 7/19/16.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types.DataType

/**
	* An extension of the [[UnaryTransformer]] with the [[SparkTransformerWrapper]] trait
	*
	* @param uid Unique ID for this transformer
	* @tparam IN  Input data type
	* @tparam OUT Output data type
	*/
abstract class UnarySparkTransformerWrapper[IN, OUT]( override val uid: String )
	extends UnaryTransformer[IN, OUT, UnarySparkTransformerWrapper[IN, OUT]] with SparkTransformerWrapper[IN, OUT]
{
	override protected def createTransformFunc: IN => OUT = tx.transform
}

object UnarySparkTransformerWrapper
{
	def apply[IN, OUT]
	( uid: String,
	  outputType: DataType,
	  createTx: () => GenericTransformer[IN, OUT] ): UnarySparkTransformerWrapper[IN, OUT] =
		new UnarySparkTransformerWrapper[IN, OUT]( uid )
		{
			override protected def outputDataType: DataType = outputType

			override protected def createTransformer: GenericTransformer[IN, OUT] = createTx()
		}

	def apply[IN, OUT]
	( outputType: DataType,
	  createTx: () => GenericTransformer[IN, OUT] ): UnarySparkTransformerWrapper[IN, OUT] =
		apply[IN, OUT]( classOf[UnarySparkTransformerWrapper[IN, OUT]].getName + " of function " + createTx,
		                outputType,
		                createTx )
}