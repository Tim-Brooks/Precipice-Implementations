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
import net.uncontended.precipice.GuardRailBuilder;
import net.uncontended.precipice.concurrent.PrecipiceFuture;
import net.uncontended.precipice.metrics.MetricCounter;
import net.uncontended.precipice.rejected.Unrejectable;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;

public class HttpAsyncServiceTest {

    @Mock
    private AsyncHttpClient client;
    @Mock
    private Request request;
    @Mock
    private Response response;
    @Captor
    private ArgumentCaptor<AsyncCompletionHandler> handlerCaptor;

    private HttpAsyncService service;
    private GuardRail<HTTPResult, Unrejectable> guardRail;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        GuardRailBuilder<HTTPResult, Unrejectable> builder = new GuardRailBuilder<>();
        guardRail = builder.name("HTTP Client")
                .resultMetrics(new MetricCounter<>(HTTPResult.class))
                .rejectedMetrics(new MetricCounter<>(Unrejectable.class))
                .build();
        service = new HttpAsyncService(guardRail, client);
    }

    @Test
    public void testSuccessRequest() throws Exception {
        PrecipiceFuture<HTTPResult, Response> f = service.submitRequest(request);
        verify(client).executeRequest(same(request), handlerCaptor.capture());

        assertNull(f.getResult());

        AsyncCompletionHandler completionHandler = this.handlerCaptor.getValue();

        completionHandler.onCompleted(response);

        assertNull(f.getError());
        assertEquals(response, f.getValue());
        assertEquals(HTTPResult.SUCCESS, f.getResult());
        assertEquals(1, guardRail.getResultMetrics().getMetricCount(HTTPResult.SUCCESS));
    }

//    @Test
//    public void testErrorRequest() throws Exception {
//        PrecipiceFuture<Response> f = service.submitRequest(request);
//        verify(client).executeRequest(same(request), handlerCaptor.capture());
//
//        assertEquals(Status.PENDING, f.getStatus());
//
//        AsyncCompletionHandler completionHandler = this.handlerCaptor.getValue();
//
//        IOException exception = new IOException();
//        completionHandler.onThrowable(exception);
//
//        assertNull(f.result());
//        assertEquals(exception, f.error());
//        assertEquals(Status.ERROR, f.getStatus());
//        assertEquals(1, service.getActionMetrics().getMetricCountForTimePeriod(Metric.ERROR, 1, TimeUnit.SECONDS));
//    }
//
//    @Test
//    public void testTimeoutRequest() throws Exception {
//        PrecipiceFuture<Response> f = service.submitRequest(request);
//        verify(client).executeRequest(same(request), handlerCaptor.capture());
//
//        assertEquals(Status.PENDING, f.getStatus());
//
//        AsyncCompletionHandler completionHandler = this.handlerCaptor.getValue();
//
//        TimeoutException exception = new TimeoutException();
//        completionHandler.onThrowable(exception);
//
//        assertNull(f.result());
//        assertNull(f.error());
//        assertEquals(Status.TIMEOUT, f.getStatus());
//        assertEquals(1, service.getActionMetrics().getMetricCountForTimePeriod(Metric.TIMEOUT, 1, TimeUnit.SECONDS));
//    }
}
