package io.hops.examples.flink.fraud;

import io.hops.examples.flink.hsfsApi.SourceTransaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/*
public class TransactionsDeserializer implements DeserializationSchema<SourceTransaction>  {
  @Override
  public SourceTransaction deserialize(byte[] message) throws IOException {
    SpecificDatumReader<SourceTransaction> reader = new SpecificDatumReader<>(SourceTransaction.SCHEMA$);
    BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(message), null);
    return reader.read(null, binaryDecoder);
  }
  
  @Override
  public boolean isEndOfStream(SourceTransaction sourceTransaction) {
    return false;
  }
  
  @Override
  public TypeInformation<SourceTransaction> getProducedType() {
    return TypeInformation.of(SourceTransaction.class);
  }
}
*/

public class TransactionsDeserializer implements KafkaDeserializationSchema<SourceTransaction> {
  
  @Override
  public boolean isEndOfStream(SourceTransaction sourceTransaction) {
    return false;
  }
  
  @Override
  public SourceTransaction deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    /*
    byte[] message = consumerRecord.value();
    SpecificDatumReader<SourceTransaction> reader = new SpecificDatumReader<>(SourceTransaction.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message,null);
    return reader.read(null,decoder);
     */
    byte[] messageKey = consumerRecord.key();
    byte[] message = consumerRecord.value();
    long offset = consumerRecord.offset();
    long timestamp = consumerRecord.timestamp();
    DeserializationSchema<SourceTransaction> deserializer =
      AvroDeserializationSchema.forSpecific(SourceTransaction.class);
    return deserializer.deserialize(message);
  }
  
  @Override
  public TypeInformation<SourceTransaction> getProducedType() {
    return TypeInformation.of(SourceTransaction.class);
  }
}
