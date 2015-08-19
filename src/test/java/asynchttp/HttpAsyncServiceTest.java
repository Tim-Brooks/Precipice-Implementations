/*
 * Copyright 2014 Timothy Brooks
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

package asynchttp;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import net.uncontended.precipice.ServiceProperties;
import net.uncontended.precipice.concurrent.PrecipiceFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class HttpAsyncServiceTest {

    @Mock
    private AsyncHttpClient client;

    private HttpAsyncService service;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        service = new HttpAsyncService("AsyncClient", new ServiceProperties(), client);
    }

    @Test
    public void testThing() {
        PrecipiceFuture<Response> f = service.submitRequest(null);

    }
}
