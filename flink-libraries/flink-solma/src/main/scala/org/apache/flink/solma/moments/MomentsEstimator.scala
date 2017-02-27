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

package org.apache.flink.solma.moments

import breeze.linalg
import eu.proteus.flink.annotaton.Proteus
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.solma.pipeline.{StreamFitOperation, StreamTransformer, TransformDataStreamOperation}
import org.apache.flink.solma.utils.FlinkSolmaUtils
import org.apache.flink.streaming.api.scala._

@Proteus
class MomentsEstimator extends StreamTransformer[MomentsEstimator] {
}

object MomentsEstimator {

  // ====================================== Parameters =============================================

  // ==================================== Factory methods ==========================================

  def apply(): MomentsEstimator = {
    new MomentsEstimator()
  }

  // ==================================== Operations ==========================================

  implicit def fitNoOp[T] = {
    new StreamFitOperation[MomentsEstimator, T]{
      override def fit(
          instance: MomentsEstimator,
          fitParameters: ParameterMap,
          input: DataStream[T])
        : Unit = {}
    }
  }

  implicit def transformMomentsEstimators[T <: Vector] = {
    new TransformDataStreamOperation[MomentsEstimator, T, (Long, linalg.Vector[Double], linalg.Vector[Double])]{
      override def transformDataStream(
        instance: MomentsEstimator,
        transformParameters: ParameterMap,
        input: DataStream[T])
        : DataStream[(Long, linalg.Vector[Double], linalg.Vector[Double])] = {
        val resultingParameters = instance.parameters ++ transformParameters
        val statefulStream = FlinkSolmaUtils.ensureKeyedStream[T](input)
        statefulStream.mapWithState((in, state: Option[(Long, linalg.Vector[Double], linalg.Vector[Double])]) => {
          val (element, _) = in
          val x = element.asBreeze
          val metrics = state match {
            case Some(curr) => {
              val (counter, mean, variance) = curr
              val scalingFactor = counter.toDouble / (counter + 1.0)
              val newMean = mean :*= scalingFactor
              newMean :+= (x :/ (counter + 1.0))
              val newVariance = variance :* scalingFactor
              newVariance :+= ((x - newMean) :^= 2.0)
              newVariance :/= counter.toDouble
              (counter + 1L, newMean, newVariance)
            }
            case None => {
              (
                1L,
                x.copy,
                linalg.DenseVector.zeros[Double](x.size)
              )
            }
          }
          (metrics, Some(metrics))
        })
      }
    }
  }


}
