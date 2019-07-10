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
	* Created by Tristan Nixon on 2019-02-10.
	*/
package org.memeticlabs.spark.pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class ColumnRenamer( override val uid: String ) extends Transformer
{
	def this() = this( Identifiable.randomUID("ColumnRenamer") )

	override def copy( extra: ParamMap ): Transformer = defaultCopy(extra)

	/** Params */

	/** @group param */
	final val colNameMap = new Param[Map[String,String]]( this,
	                                                      "colNameMap",
	                                                      "Maps columns to be renamed to their new names" )

	/** @group getParam */
	def getColNameMap: Map[String,String] = $(colNameMap)

	/** @group setParam */
	def setColNameMap( rename: Map[String,String] ): ColumnRenamer.this.type = set( colNameMap, rename )

	/** @group setParam */
	def withColumnRenamed( from: String, to: String ): ColumnRenamer.this.type =
		set( colNameMap, $(colNameMap) + (from -> to) )

	/** Transform */

	override def transformSchema( schema: StructType ): StructType =
	{
		// make sure that the from columns exist
		$(colNameMap).keys.foreach( col => require( schema.fieldNames.contains(col),
		                                            s"Table does not contain column $col" ) )

		// rename the columns
		StructType( schema.map( field =>
			$(colNameMap).get(field.name) match {
				case Some(to) => field.copy( name = to )
				case None => field
			}))
	}

	private def rename( dataset: DataFrame, nameMap: (String, String) ): DataFrame =
		dataset.withColumnRenamed( nameMap._1, nameMap._2 )

	override def transform( dataset: Dataset[_] ): DataFrame =
		$(colNameMap).foldLeft(dataset.toDF())( rename )
}
