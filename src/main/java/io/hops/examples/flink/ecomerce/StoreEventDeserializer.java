package io.hops.examples.flink.ecomerce;

import io.hops.examples.flink.examples.StoreEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class StoreEventDeserializer implements KafkaDeserializationSchema<StoreEvent> {
  
  @Override
  public boolean isEndOfStream(StoreEvent storeEvent) {
    return false;
  }
  
  @Override
  public StoreEvent deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    byte[] messageKey = consumerRecord.key();
    byte[] message = consumerRecord.value();
    long offset = consumerRecord.offset();
    long timestamp = consumerRecord.timestamp();
    DeserializationSchema<StoreEvent> deserializer =
      AvroDeserializationSchema.forSpecific(StoreEvent.class);
    return deserializer.deserialize(message);
  }
  
  @Override
  public TypeInformation<StoreEvent> getProducedType() {
    return TypeInformation.of(StoreEvent.class);
  }
}
