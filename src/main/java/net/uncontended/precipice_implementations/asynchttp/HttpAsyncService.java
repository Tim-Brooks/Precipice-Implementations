/*
 * Copyright 2015 Timothy Brooks
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
 */

package net.uncontended.precipice_implementations.asynchttp;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.Response;
import net.uncontended.precipice.AbstractService;
import net.uncontended.precipice.AsyncService;
import net.uncontended.precipice.ResilientAction;
import net.uncontended.precipice.ServiceProperties;
import net.uncontended.precipice.concurrent.Eventual;
import net.uncontended.precipice.concurrent.PrecipiceFuture;
import net.uncontended.precipice.concurrent.PrecipicePromise;
import net.uncontended.precipice.concurrent.PrecipiceSemaphore;
import net.uncontended.precipice.metrics.ActionMetrics;
import net.uncontended.precipice.metrics.Metric;
import net.uncontended.precipice.timeout.ActionTimeoutException;

import java.util.concurrent.TimeoutException;


public class HttpAsyncService extends AbstractService implements AsyncService {

    private final AsyncHttpClient client;

    public HttpAsyncService(String name, ServiceProperties properties, AsyncHttpClient client) {
        super(name, properties.circuitBreaker(), properties.actionMetrics(), properties.semaphore());
        this.client = client;
    }

    public PrecipiceFuture<Response> submitRequest(Request request) {
        return submit(new ResponseAction(request), -1L);
    }

    @Override
    public <T> PrecipiceFuture<T> submit(final ResilientAction<T> action, long millisTimeout) {
        final Eventual<T> eventual = new Eventual<>();
        complete(action, eventual, millisTimeout);
        return eventual;
    }


    @Override
    public <T> void complete(ResilientAction<T> action, PrecipicePromise<T> promise, long millisTimeout) {
        acquirePermitOrRejectIfActionNotAllowed();

        final ServiceRequest<T> asyncRequest = (ServiceRequest<T>) action;
        client.executeRequest(asyncRequest.getRequest(), new CompletionHandler<>(System.nanoTime(), actionMetrics,
                semaphore, asyncRequest, promise));
    }

    @Override
    public void shutdown() {
        isShutdown.set(true);
    }

    private static class ResponseAction extends ServiceRequest<Response> {
        public ResponseAction(Request request) {
            super(request);
        }

        @Override
        public Response run() throws Exception {
            return response;
        }
    }

    private static class CompletionHandler<T> extends AsyncCompletionHandler<Void> {
        private final long startTime;
        private final ServiceRequest<T> asyncRequest;
        private final PrecipicePromise<T> promise;
        private final PrecipiceSemaphore semaphore;
        private final ActionMetrics actionMetrics;

        public CompletionHandler(long startTime, ActionMetrics actionMetrics, PrecipiceSemaphore semaphore,
                                 ServiceRequest<T> asyncRequest, PrecipicePromise<T> promise) {
            this.startTime = startTime;
            this.actionMetrics = actionMetrics;
            this.asyncRequest = asyncRequest;
            this.promise = promise;
            this.semaphore = semaphore;
        }

        @Override
        public Void onCompleted(Response response) throws Exception {
            asyncRequest.setResponse(response);
            try {
                T result = asyncRequest.run();
                long endTime = System.nanoTime();
                actionMetrics.incrementMetricAndRecordLatency(Metric.SUCCESS, endTime - startTime, endTime);
                promise.complete(result);
            } catch (ActionTimeoutException e) {
                long endTime = System.nanoTime();
                actionMetrics.incrementMetricAndRecordLatency(Metric.TIMEOUT, endTime - startTime, endTime);
                promise.completeWithTimeout();
            } catch (Exception e) {
                long endTime = System.nanoTime();
                actionMetrics.incrementMetricAndRecordLatency(Metric.ERROR, endTime - startTime, endTime);
                promise.completeExceptionally(e);
            } finally {
                semaphore.releasePermit();
            }
            return null;
        }

        @Override
        public void onThrowable(Throwable t) {
            long endTime = System.nanoTime();
            if (t instanceof TimeoutException) {
                actionMetrics.incrementMetricAndRecordLatency(Metric.TIMEOUT, endTime - startTime, endTime);
                promise.completeWithTimeout();
            } else {
                actionMetrics.incrementMetricAndRecordLatency(Metric.ERROR, endTime - startTime, endTime);
                promise.completeExceptionally(t);
            }
            semaphore.releasePermit();
        }
    }
}
