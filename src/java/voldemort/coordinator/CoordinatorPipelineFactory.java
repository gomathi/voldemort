/*
 * Copyright 2008-2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.coordinator;

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import voldemort.utils.ByteArray;
import voldemort.utils.DaemonThreadFactory;

/**
 * A PipelineFactory implementation to setup the Netty Pipeline in the
 * Coordinator
 * 
 */
public class CoordinatorPipelineFactory implements ChannelPipelineFactory {

    private Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap;

    // TODO: Fix in the next pass
    private CoordinatorErrorStats errorStats = null;

    ThreadFactory threadFactory = new DaemonThreadFactory("Voldemort-Coordinator-Thread");
    private final ThreadPoolExecutor threadPoolExecutor;
    private final CoordinatorExecutionHandler coordinatorExecutionHandler;
    private final CoordinatorMetadata coordinatorMetadata;

    public CoordinatorPipelineFactory(Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap,
                                      CoordinatorMetadata coordinatorMetadata,
                                      CoordinatorConfig coordinatorConfig,
                                      CoordinatorErrorStats errorStats) {
        this.fatClientMap = fatClientMap;
        this.errorStats = errorStats;
        this.threadPoolExecutor = new ThreadPoolExecutor(coordinatorConfig.getCoordinatorCoreThreads(),
                                                         coordinatorConfig.getCoordinatorMaxThreads(),
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(coordinatorConfig.getCoordinatorQueuedRequestsSize()),
                                                         threadFactory);
        this.coordinatorMetadata = coordinatorMetadata;
        coordinatorExecutionHandler = new CoordinatorExecutionHandler(threadPoolExecutor,
                                                                      this.coordinatorMetadata);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new CoordinatorRestRequestHandler(fatClientMap));
        pipeline.addLast("storageExecutionHandler", coordinatorExecutionHandler);
        return pipeline;
    }
}