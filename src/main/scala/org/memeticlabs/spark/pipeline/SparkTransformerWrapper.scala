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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 9/25/16.
	*/
package org.memeticlabs.spark.pipeline

/**
	* A trait to embed a [[GenericTransformer]] in a a Spark [[org.apache.spark.ml.Transformer]]
	*
	* @tparam IN  Input data type
	* @tparam OUT Output data type
	*/
trait SparkTransformerWrapper[IN, OUT] extends Serializable
{
	protected lazy val tx = createTransformer

	protected def createTransformer: GenericTransformer[IN, OUT]
}
