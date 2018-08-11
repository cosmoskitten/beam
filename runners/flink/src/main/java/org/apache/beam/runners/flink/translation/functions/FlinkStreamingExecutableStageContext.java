package org.apache.beam.runners.flink.translation.functions;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DockerJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.flink.api.common.functions.RuntimeContext;

/** Implementation of a {@link FlinkExecutableStageContext} for streaming. */
public class FlinkStreamingExecutableStageContext implements FlinkExecutableStageContext {
  private final JobBundleFactory jobBundleFactory;

  FlinkStreamingExecutableStageContext(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.forStage(executableStage);
  }

  private static FlinkExecutableStageContext create(JobInfo jobInfo) throws Exception {
    JobBundleFactory jobBundleFactory = DockerJobBundleFactory.create(jobInfo);
    return new FlinkStreamingExecutableStageContext(jobBundleFactory);
  }

  @Override
  public StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage, RuntimeContext runtimeContext) {

    if (!executableStage.getSideInputs().isEmpty()) {
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          checkNotNull(
              FlinkStreamingSideInputHandlerFactory.getFor(executableStage, runtimeContext));
      try {
        return StateRequestHandlers.forSideInputHandlerFactory(
            ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return StateRequestHandler.unsupported();
    }
  }

  @Override
  protected void finalize() throws Exception {
    jobBundleFactory.close();
  }

  /** StreamingFactory. * */
  public enum StreamingFactory implements Factory {
    INSTANCE;

    @SuppressWarnings("Immutable") // observably immutable
    private final LoadingCache<JobInfo, FlinkExecutableStageContext> cachedContexts;

    StreamingFactory() {
      cachedContexts =
          CacheBuilder.newBuilder()
              .weakValues()
              .build(
                  new CacheLoader<JobInfo, FlinkExecutableStageContext>() {
                    @Override
                    public FlinkExecutableStageContext load(JobInfo jobInfo) throws Exception {
                      return create(jobInfo);
                    }
                  });
    }

    @Override
    public FlinkExecutableStageContext get(JobInfo jobInfo) {
      return cachedContexts.getUnchecked(jobInfo);
    }
  }
}
