/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.errors

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status

import org.apache.spark.status.KVUtils.MetadataMismatchException
import org.apache.spark.status.api.v1.{BadParameterException, ForbiddenException, NotFoundException, ServiceUnavailable}

private[spark] object ExecutionErrors {
  def failToGetApplicationInfo(): Throwable = {
    new NoSuchElementException("Failed to get the application information. " +
      "If you are starting up Spark, please wait a while until it's ready.")
  }

  def noStageWithId(stageId: Int): Throwable = {
    new NoSuchElementException(s"No stage with id $stageId")
  }

  def failToGetApplicationSummary(): Throwable = {
    new NoSuchElementException("Failed to get the application summary. " +
      "If you are starting up Spark, please wait a while until it's ready.")
  }

  def metadataMismatch(): Throwable = {
    new MetadataMismatchException()
  }

  def indexOutOfBound(idx: Int): Throwable = {
    new IndexOutOfBoundsException(idx.toString)
  }

  def noSuchElement(): Throwable = {
    new NoSuchElementException()
  }

  def notAuthorizedUser(user: String): Throwable = {
    new ForbiddenException(raw"""user "$user" is not authorized""")
  }

  def notFoundAppKey(appKey: String): Throwable = {
    new NotFoundException(s"no such app: $appKey")
  }

  def notFoundJobId(jobId: Int): Throwable = {
    new NotFoundException("unknown job: " + jobId)
  }

  def badParameterError(url: String): Throwable = {
    new BadParameterException(
      s"Invalid executorId: neither '$url' nor number.")
  }

  def serviceUnavailableError(): Throwable = {
    new ServiceUnavailable("Thread dumps not available through the history server.")
  }

  def notFoundThread(): Throwable = {
    new NotFoundException("No thread dump is available.")
  }

  def notFoundHttpRequest(uri: String): Throwable = {
    new NotFoundException(uri)
  }

  def notFoundExecutor(): Throwable = {
    new NotFoundException("Executor does not exist.")
  }

  def executorNotActive(): Throwable = {
    new BadParameterException("Executor is not active.")
  }

  def notFoundRdd(rddId: Int): Throwable = {
    new NotFoundException(s"no rdd found w/ id $rddId")
  }

  def serviceUnavailableError(appId: String): Throwable = {
    new ServiceUnavailable(s"Event logs are not available for app: $appId.")
  }

  def notFoundAppId(appId: String): Throwable = {
    new ServiceUnavailable(s"Event logs are not available for app: $appId.")
  }

  def notFoundAppIddAndAttemptId(appId: String, attemptId: String): Throwable = {
    new NotFoundException(s"unknown app $appId, attempt $attemptId")
  }

  def notFoundStageId(stageId: Int): Throwable = {
    new NotFoundException(s"unknown stage: $stageId")
  }

  def notFound(msg: String): Throwable = {
    new NotFoundException(msg)
  }

  def noTasksReportMetrics(stageId: Int, stageAttemptId: Int): Throwable = {
    new NotFoundException(s"No tasks reported metrics for $stageId / $stageAttemptId yet.")
  }

  def badParameterErrors(s: String): Throwable = {
    new BadParameterException("quantiles", "double", s)
  }

  def webApplicationError(originalValue: String): Throwable = {
    new WebApplicationException(
      Response
        .status(Status.BAD_REQUEST)
        .entity("Couldn't parse date: " + originalValue)
        .build()
    )
  }
}
