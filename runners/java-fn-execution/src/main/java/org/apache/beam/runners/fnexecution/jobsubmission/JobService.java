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
package org.apache.beam.runners.fnexecution.jobsubmission;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JobService that prepares and runs jobs on behalf of a client using a {@link JobInvoker}.
 */
public class JobService extends JobServiceGrpc.JobServiceImplBase implements FnService {
  private static final Logger LOG = LoggerFactory.getLogger(JobService.class);

  public static JobService create(
      ArtifactStagingServiceProvider artifactStagingServiceProvider, JobInvoker invoker) {
    return new JobService(artifactStagingServiceProvider, invoker);
  }

  private final ConcurrentMap<String, JobPreparation> preparations;
  private final ConcurrentMap<String, JobInvocation> invocations;
  private final ArtifactStagingServiceProvider artifactStagingServiceProvider;
  private final JobInvoker invoker;

  private JobService(
      ArtifactStagingServiceProvider artifactStagingServiceProvider, JobInvoker invoker) {
    this.artifactStagingServiceProvider = artifactStagingServiceProvider;
    this.invoker = invoker;

    this.preparations = new ConcurrentHashMap<>();
    this.invocations = new ConcurrentHashMap<>();
  }

  @Override
  public void prepare(
      PrepareJobRequest request,
      StreamObserver<PrepareJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", PrepareJobRequest.class.getSimpleName(), request);
      // insert preparation
      String preparationId =
          String.format("%s_%d", request.getJobName(), ThreadLocalRandom.current().nextInt());
      GrpcFnServer<ArtifactStagingService> stagingService =
          artifactStagingServiceProvider.forJob(preparationId);
      Struct pipelineOptions = request.getPipelineOptions();
      if (pipelineOptions == null) {
        LOG.trace("PIPELINE OPTIONS IS NULL");
        throw new NullPointerException("Encountered null pipeline options.");
            /*
        LOG.debug("Encountered null pipeline options.  Using default.");
        pipelineOptions = Struct.getDefaultInstance();
        */
      } else {
        LOG.trace("PIPELINE OPTIONS IS NOT NULL");
      }
      LOG.trace("PIPELINE OPTIONS {} {}", pipelineOptions.getClass(), pipelineOptions);
      JobPreparation preparation =
          JobPreparation
              .builder()
              .setId(preparationId)
              .setPipeline(request.getPipeline())
              .setOptions(pipelineOptions)
              .setStagingService(stagingService)
              .build();
      JobPreparation previous = preparations.putIfAbsent(preparationId, preparation);
      if (previous != null) {
        String errMessage =
            String.format("A job with the preparation ID \"%s\" already exists.", preparationId);
        StatusException exception = Status.NOT_FOUND.withDescription(errMessage).asException();
        responseObserver.onError(exception);
        return;
      }

      // send response
      PrepareJobResponse response =
          PrepareJobResponse
              .newBuilder()
              .setPreparationId(preparationId)
              .setArtifactStagingEndpoint(stagingService.getApiServiceDescriptor())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Could not prepare job with name {}", request.getJobName(), e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void run(
      RunJobRequest request, StreamObserver<RunJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", RunJobRequest.class.getSimpleName(), request);

      // retrieve job preparation
      String preparationId = request.getPreparationId();
      JobPreparation preparation = preparations.get(preparationId);
      if (preparation == null) {
        String errMessage = String.format("Unknown Preparation Id \"%s\".", preparationId);
        StatusException exception = Status.NOT_FOUND.withDescription(errMessage).asException();
        responseObserver.onError(exception);
        return;
      }

      // create new invocation
      JobInvocation invocation = invoker.invoke(preparation, request.getStagingToken());
      String invocationId = invocation.getId();
      invocation.start();
      invocations.put(invocationId, invocation);
      RunJobResponse response =
          RunJobResponse.newBuilder().setJobId(invocationId).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      LOG.warn("Encountered Status Exception", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Encountered Unexpected Exception", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getState(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    try {
      LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
      String invocationId = request.getJobId();
      JobInvocation invocation = invocations.get(invocationId);

      JobState.Enum state;
      synchronized (invocation) {
        state = invocation.getState();
      }

      GetJobStateResponse response = GetJobStateResponse.newBuilder().setState(state).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Encountered Unexpected Exception", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", CancelJobRequest.class.getSimpleName(), request);
      String invocationId = request.getJobId();
      JobInvocation invocation = invocations.get(invocationId);

      JobState.Enum state;
      synchronized (invocation) {
        invocation.cancel();
        state = invocation.getState();
      }

      CancelJobResponse response = CancelJobResponse.newBuilder().setState(state).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Encountered Unexpected Exception", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getStateStream(
      GetJobStateRequest request,
      StreamObserver<GetJobStateResponse> responseObserver) {
    try {
      String invocationId = request.getJobId();
      JobInvocation invocation = invocations.get(invocationId);

      Function<JobState.Enum, GetJobStateResponse> responseFunction =
          state -> GetJobStateResponse.newBuilder().setState(state).build();
      TransformStreamObserver<JobState.Enum, GetJobStateResponse> stateObserver =
          TransformStreamObserver.create(responseFunction, responseObserver);
      synchronized (invocation) {
        invocation.addStateObserver(stateObserver);
      }
    } catch (Exception e) {
      LOG.error("Encountered Unexpected Exception", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getMessageStream(
      JobMessagesRequest request,
      StreamObserver<JobMessagesResponse> responseObserver) {
    try {
      String invocationId = request.getJobId();
      JobInvocation invocation = invocations.get(invocationId);

      Function<JobState.Enum, JobMessagesResponse> stateResponseFunction =
          state ->
              JobMessagesResponse
                  .newBuilder()
                  .setStateResponse(GetJobStateResponse.newBuilder().setState(state).build())
                  .build();
      TransformStreamObserver<JobState.Enum, JobMessagesResponse> stateObserver =
          TransformStreamObserver.create(stateResponseFunction, responseObserver);

      Function<JobMessage, JobMessagesResponse> messagesResponseFunction =
          message -> JobMessagesResponse.newBuilder().setMessageResponse(message).build();
      TransformStreamObserver<JobMessage, JobMessagesResponse> messageObserver =
          TransformStreamObserver.create(messagesResponseFunction, responseObserver);

      synchronized (invocation) {
        invocation.addStateObserver(stateObserver);
        invocation.addMessageObserver(messageObserver);
      }
    } catch (Exception e) {
      LOG.error("Encountered Unexpected Exception", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void close() throws Exception {
    for (JobPreparation preparation : ImmutableList.copyOf(preparations.values())) {
      try {
        preparation.stagingService().close();
      } catch (Exception e) {
        LOG.warn("Exception while closing job {}", preparation);
      }
    }
  }

  /**
   * Utility class that transforms observations into one that an output observer can accept.
   *
   * <p>This class synchronizes on outputObserver when handling it, allowing for multiple
   * TransformStreamObservers to multiplex to a single outputObserver.
   *
   * @param <T1> Input Type
   * @param <T2> Output Type
   */
  private static class TransformStreamObserver<T1, T2> implements StreamObserver<T1> {

    /**
     * Create a new TransformStreamObserver.
     *
     * @param transform The function used to transform observations.
     * @param outputObserver The observer to forward transform outputs to.
     */
    public static <T1, T2> TransformStreamObserver<T1, T2> create(
        Function<T1, T2> transform, StreamObserver<T2> outputObserver) {
      return new TransformStreamObserver<>(transform, outputObserver);
    }

    private final Function<T1, T2> transform;
    private final StreamObserver<T2> outputObserver;

    private TransformStreamObserver(Function<T1, T2> transform, StreamObserver<T2> outputObserver) {
      this.transform = transform;
      this.outputObserver = outputObserver;
    }

    @Override
    public void onNext(T1 i) {
      T2 o = transform.apply(i);
      synchronized (outputObserver) {
        outputObserver.onNext(o);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      synchronized (outputObserver) {
        outputObserver.onError(throwable);
      }
    }

    @Override
    public void onCompleted() {
      synchronized (outputObserver) {
        outputObserver.onCompleted();
      }
    }
  }

}
