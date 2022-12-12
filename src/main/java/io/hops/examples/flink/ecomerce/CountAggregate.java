package io.hops.examples.flink.ecomerce;

import io.hops.examples.flink.examples.StoreEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import java.time.Instant;

public class CountAggregate implements AggregateFunction<StoreEvent, Tuple6<Long, Long, Long, Long, Long, Long>,
  Tuple6<Long, Long, Long, Long, Long, Long>>  {
  
  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> createAccumulator() {
    return new Tuple6<>(0L,0L,0L,0L,0L,0L);
  }
  
  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> add(StoreEvent record,
    Tuple6<Long, Long, Long, Long, Long, Long> accumulator) {
    return new Tuple6<>(
      accumulator.f0 + 1,
      Instant.now().toEpochMilli(),
      Math.max(accumulator.f2, record.getReceivedTs()),
      Math.max(accumulator.f3, record.getDeserializationTimestamp()),
      Math.max(accumulator.f4, record.getKafkaCommitTimestamp()),
      0L);
  }
  
  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> getResult(Tuple6<Long, Long, Long, Long, Long, Long> accumulator) {
    return new Tuple6<>(
      accumulator.f0,
      accumulator.f1,
      accumulator.f2,
      accumulator.f3,
      accumulator.f4,
      accumulator.f5);
  }
  
  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> merge(Tuple6<Long, Long, Long, Long, Long, Long> accumulator,
    Tuple6<Long, Long, Long, Long, Long, Long> accumulator1) {
    return new Tuple6<>(
      accumulator.f0 + accumulator1.f0,
      accumulator1.f1,
      accumulator1.f2,
      Math.max(accumulator.f3, accumulator1.f3),
      Math.max(accumulator.f4, accumulator1.f4),
      accumulator1.f5);
  }
}