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
package org.memeticlabs.spark.ml.utils.transformers

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class FunctionTransformerWrapper( override val uid: String )
	extends Transformer with HasOutputCol
{
	override def copy( extra: ParamMap ): Transformer = defaultCopy( extra )

	/** Params */
	/** @group param */
	final val inputCols = new Param[Seq[String]]( this,
	                                              "inputCols",
	                                              "The list of input columns for this function" )
	setDefault( inputCols, Seq[String]() )

	/** @group getParam */
	final def getInputCols: Seq[String] = $( inputCols )

	/** @group setParam */
	final def setInputCols( values: Seq[String] ): this.type =
		set( inputCols, values )

	final def addInputCol( value: String ): this.type =
		set( inputCols, $( inputCols ) :+ value )

	/** @group setParam */
	final def setOutput( value: String ): this.type =
		set( outputCol, value )

	/**
	 * The transformer function to be run on input columns to
	 * produce the output column
	 */
	protected val transformerUDF: UserDefinedFunction

	/** Transform */
	override def transformSchema( schema: StructType ): StructType =
	{
		// make sure we have the right number of input columns
		transformerUDF.inputTypes match {
			case None => require( $( inputCols ) == null || $( inputCols ).isEmpty,
			                      s"UDF for FunctionTransformer ${uid} takes no arguments, "+
			                      s"but ${$( inputCols ).size} input columns specified" )
			case Some( udfArgs ) => require( $( inputCols ).size == udfArgs.size,
			                                 s"UDF for FunctionTransformer ${uid} takes ${udfArgs.size} arguments, "+
			                                 s"but ${$( inputCols ).size} input columns specified" )
		}

		// validate each input column
		if(transformerUDF.inputTypes.isDefined)
			$( inputCols ).zip( transformerUDF.inputTypes.get )
			              .foreach( input =>
				                        schema.find( _.name.equals( input._1 ) ) match
				                        {
					                        case Some( column ) =>
						                        require( column.dataType.equals( input._2 ),
						                                 s"FunctionTransformer ${uid}: expected input column ${column.name} to have type ${input._2}, "+
						                                 s"found ${column.dataType} instead!" )
					                        case None =>
						                        throw new IllegalArgumentException( s"FunctionTransformer ${uid}: no column named ${input._1} found in schema!" )} )

		// should not have output column
		if(schema.fieldNames.contains( $( outputCol ) ))
			throw new IllegalArgumentException( s"FunctionTransformer ${uid}: output column ${$( outputCol )} already exists." )

		// add the output column to the schema
		val outputFields =
			schema.fields :+ StructField( $( outputCol ),
			                              transformerUDF.dataType,
			                              nullable = false )
		StructType( outputFields )
	}

	override def transform( ds: Dataset[_] ): DataFrame =
	{
		transformSchema( ds.schema, logging = true )

		// apply the transformer function to the input columns
		ds.withColumn( $( outputCol ),
		               transformerUDF( $( inputCols ).map( col ): _* ) )
	}
}

object FunctionTransformerWrapper
{
	def apply( typeName: String,
	           inputColumns: Seq[String],
	           outputColumn: String,
	           txUDF: UserDefinedFunction ): FunctionTransformerWrapper =
		new FunctionTransformerWrapper( Identifiable.randomUID( typeName ) )
	{
		override protected val transformerUDF: UserDefinedFunction = txUDF
	}.setInputCols( inputColumns )
   .setOutput( outputColumn )

	def apply[RT: TypeTag]( typeName: String,
	                        outputColumn: String,
	                        f: () => RT ): FunctionTransformerWrapper =
		apply( typeName, Seq(), outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag]( typeName: String,
	                        inputColumn: String,
	                        outputColumn: String,
	                        f: A1 => RT ): FunctionTransformerWrapper =
		apply( typeName, Seq(inputColumn), outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3, A4) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag,
	          A5: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3, A4, A5) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag,
	          A5: TypeTag,
	          A6: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3, A4, A5, A6) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag,
	          A5: TypeTag,
	          A6: TypeTag,
	          A7: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3, A4, A5, A6, A7) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag,
	          A5: TypeTag,
	          A6: TypeTag,
	          A7: TypeTag,
	          A8: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3, A4, A5, A6, A7, A8) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag,
	          A5: TypeTag,
	          A6: TypeTag,
	          A7: TypeTag,
	          A8: TypeTag,
	          A9: TypeTag]( typeName: String,
	                        inputColumns: Seq[String],
	                        outputColumn: String,
	                        f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )

	def apply[RT: TypeTag,
	          A1: TypeTag,
	          A2: TypeTag,
	          A3: TypeTag,
	          A4: TypeTag,
	          A5: TypeTag,
	          A6: TypeTag,
	          A7: TypeTag,
	          A8: TypeTag,
	          A9: TypeTag,
	          A10: TypeTag]( typeName: String,
	                         inputColumns: Seq[String],
	                         outputColumn: String,
	                         f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => RT ): FunctionTransformerWrapper =
		apply( typeName, inputColumns, outputColumn, udf( f ) )
}