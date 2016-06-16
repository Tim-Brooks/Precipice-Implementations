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

import net.uncontended.precipice.GuardRail;
import net.uncontended.precipice.concurrent.Eventual;
import net.uncontended.precipice.concurrent.PrecipiceFuture;
import net.uncontended.precipice.concurrent.PrecipicePromise;
import net.uncontended.precipice.factories.Asynchronous;
import net.uncontended.precipice.timeout.PrecipiceTimeoutException;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.util.concurrent.TimeoutException;


public class HttpAsyncService {

    private final GuardRail<HTTPResult, ?> guardRail;
    private final AsyncHttpClient client;

    public HttpAsyncService(GuardRail<HTTPResult, ?> guardRail, AsyncHttpClient client) {
        this.guardRail = guardRail;
        this.client = client;
    }

    public PrecipiceFuture<HTTPResult, Response> submitRequest(Request request) {
        Eventual<HTTPResult, Response> promise = Asynchronous.acquireSinglePermitAndPromise(guardRail);
        client.executeRequest(request, new CompletionHandler(promise));
        return promise.future();
    }

    public void complete(Request request, PrecipicePromise<HTTPResult, Response> completable) {
        Eventual<HTTPResult, Response> promise = Asynchronous.acquireSinglePermitAndPromise(guardRail, completable);
        client.executeRequest(request, new CompletionHandler(promise));
    }

    private static class CompletionHandler extends AsyncCompletionHandler<Void> {
        private final PrecipicePromise<HTTPResult, Response> promise;

        public CompletionHandler(PrecipicePromise<HTTPResult, Response> promise) {
            this.promise = promise;
        }

        @Override
        public Void onCompleted(Response response) throws Exception {
            promise.complete(HTTPResult.SUCCESS, response);
            return null;
        }

        @Override
        public void onThrowable(Throwable t) {
            if (t instanceof TimeoutException) {
                promise.completeExceptionally(HTTPResult.TIMEOUT, new PrecipiceTimeoutException(t));
            } else {
                promise.completeExceptionally(HTTPResult.ERROR, t);
            }
        }
    }
}
