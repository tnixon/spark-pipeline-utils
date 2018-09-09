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
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 10/5/16.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
	* A collection of Transformers that select/drop specified columns
	*/
abstract class AbstractColumnSelector( override val uid: String ) extends Transformer
{
	override def copy( extra: ParamMap ): Transformer = defaultCopy( extra )

	// Params

	/** @group param */
	val colNames = new Param[Array[String]]( this, "colNames", "Names of columns" )

	/** @group getParam */
	def getColNames: Array[String] = $( colNames )

	/** @group setParam */
	def setColNames( cols: Array[String] ): AbstractColumnSelector.this.type = set( colNames, cols )
}

/**
	* Transformer that will select out specified columns
	*
	* @param uid unique ID of this transformer
	*/
class ColumnSelector( uid: String ) extends AbstractColumnSelector( uid )
{
	def this( ) = this( Identifiable.randomUID( "ColumnSelector" ) )

	override def transformSchema( schema: StructType ): StructType =
	{
		$( colNames ).foreach( col => require( schema.fieldNames.contains( col ),
		                                       s"The column $col is not present!" ) )
		StructType( schema.filter( col => $( colNames ).contains( col.name ) ) )
	}

	override def transform( dataset: Dataset[_] ): DataFrame =
	{
		transformSchema( dataset.schema, logging = true )
		val cols = $( colNames ).map( col => dataset( col ) )
		dataset.select( cols: _* )
	}
}

/**
	* Transformer that will drop specified columns
	*
	* @param uid unique ID of this transformer
	*/
class ColumnDropper( uid: String ) extends AbstractColumnSelector( uid )
{
	def this( ) = this( Identifiable.randomUID( "ColumnDropper" ) )

	override def transformSchema( schema: StructType ): StructType =
	{
		$( colNames ).foreach( col => require( schema.fieldNames.contains( col ),
		                                       s"The column $col is not present!" ) )
		StructType( schema.filterNot( col => $( colNames ).contains( col.name ) ) )
	}

	override def transform( dataset: Dataset[_] ): DataFrame =
	{
		transformSchema( dataset.schema, logging = true )
		$( colNames ).foldLeft( dataset.toDF )( ( cur, col ) => cur.drop( col ) )
	}
}
