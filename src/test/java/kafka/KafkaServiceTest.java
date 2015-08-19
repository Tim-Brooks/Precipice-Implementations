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

package kafka;

import net.uncontended.precipice.ServiceProperties;
import net.uncontended.precipice.Status;
import net.uncontended.precipice.concurrent.PrecipiceFuture;
import net.uncontended.precipice.metrics.Metric;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaServiceTest {

    private MockProducer producer = new MockProducer(false);
    private KafkaService<byte[], byte[]> service;

    @Before
    public void setUp() {
        service = new KafkaService<>("Kafka", new ServiceProperties(), producer);

    }

    @Test
    public void testSuccessfulSend() {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic", "key".getBytes(), "value".getBytes());

        PrecipiceFuture<RecordMetadata> future = service.sendRecordAction(producerRecord);

        assertEquals(Status.PENDING, future.getStatus());

        producer.completeNext();

        assertEquals(Status.SUCCESS, future.getStatus());
        assertEquals(1, service.getActionMetrics().getMetricCountForTimePeriod(Metric.SUCCESS, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testFailedSend() {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic", "key".getBytes(), "value".getBytes());

        PrecipiceFuture<RecordMetadata> future = service.sendRecordAction(producerRecord);

        assertEquals(Status.PENDING, future.getStatus());

        NetworkException e = new NetworkException();
        producer.errorNext(e);

        assertEquals(Status.ERROR, future.getStatus());
        assertEquals(e, future.error());
        assertEquals(1, service.getActionMetrics().getMetricCountForTimePeriod(Metric.ERROR, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testTimeout() {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic", "key".getBytes(), "value".getBytes());

        PrecipiceFuture<RecordMetadata> future = service.sendRecordAction(producerRecord);

        assertEquals(Status.PENDING, future.getStatus());

        TimeoutException e = new TimeoutException();
        producer.errorNext(e);

        assertEquals(Status.TIMEOUT, future.getStatus());
        assertEquals(1, service.getActionMetrics().getMetricCountForTimePeriod(Metric.TIMEOUT, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testKafkaException() {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        service = new KafkaService<>("Kafka", new ServiceProperties(), producer);

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic", "key".getBytes(), "value".getBytes());

        when(producer.send(eq(producerRecord), any(Callback.class))).thenThrow(new KafkaException());

        PrecipiceFuture<RecordMetadata> future = service.sendRecordAction(producerRecord);

        assertEquals(Status.ERROR, future.getStatus());
        assertEquals(1, service.getActionMetrics().getMetricCountForTimePeriod(Metric.ERROR, 1, TimeUnit.SECONDS));
    }
}
