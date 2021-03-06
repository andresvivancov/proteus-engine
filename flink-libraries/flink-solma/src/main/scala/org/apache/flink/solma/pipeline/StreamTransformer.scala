/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.solma.pipeline

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.runtime.universe._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.solma.utils.FlinkSolmaUtils
import org.apache.flink.streaming.api.scala._

import scala.reflect.ClassTag

trait StreamTransformer[Self] extends StreamEstimator[Self] {
  that: Self =>

  def transform[Input, Output](
      input: DataStream[Input],
      transformParameters: ParameterMap = ParameterMap.Empty)
      (implicit transformOperation: TransformDataStreamOperation[Self, Input, Output])
    : DataStream[Output] = {
    FlinkSolmaUtils.registerFlinkMLTypes(input.executionEnvironment)
    transformOperation.transformDataStream(that, transformParameters, input)
  }

}

object StreamTransformer {
  implicit def defaultTransformDataSetOperation[
      Instance <: StreamEstimator[Instance],
      Model,
      Input,
      Output](
      implicit transformOperation: StreamTransformOperation[Instance, Model, Input, Output],
      outputTypeInformation: TypeInformation[Output],
      outputClassTag: ClassTag[Output])
    : TransformDataStreamOperation[Instance, Input, Output] = {
    new TransformDataStreamOperation[Instance, Input, Output] {
      override def transformDataStream(
          instance: Instance,
          transformParameters: ParameterMap,
          input: DataStream[Input])
        : DataStream[Output] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val model = transformOperation.getModel(instance, resultingParameters)

        input.map(element => {
          transformOperation.transform(element, model)
        })
      }
    }
  }
}

trait TransformDataStreamOperation[Instance, Input, Output] extends Serializable {
  def transformDataStream(
      instance: Instance,
      transformParameters: ParameterMap,
      input: DataStream[Input])
    : DataStream[Output]
}

trait StreamTransformOperation[Instance, Model, Input, Output] extends Serializable {

  /** Retrieves the model of the [[Transformer]] for which this operation has been defined.
    *
    * @param instance
    * @param transformParemters
    * @return
    */
  def getModel(instance: Instance, transformParemters: ParameterMap): Model

  /** Transforms a single element with respect to the model associated with the respective
    * [[Transformer]]
    *
    * @param element
    * @param model
    * @return
    */
  def transform(element: Input, model: Model): Output
}
