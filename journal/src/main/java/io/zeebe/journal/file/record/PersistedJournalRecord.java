package io.zeebe.journal.file.record;

import io.zeebe.journal.JournalRecord;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class PersistedJournalRecord implements JournalRecord {
  final PersistedJournalRecordMetadata metadata;
  final PersistedIndexedRecord record;

  public PersistedJournalRecord(final DirectBuffer buffer) {
    metadata = new PersistedJournalRecordMetadata(buffer);
    final var metadataLength = metadata.getLength();
    record =
        new PersistedIndexedRecord(
            new UnsafeBuffer(buffer, metadataLength, buffer.capacity() - metadataLength));
  }

  public PersistedJournalRecord(final ByteBuffer buffer) {
    metadata = new PersistedJournalRecordMetadata(new UnsafeBuffer(buffer));
    final var metadataLength = metadata.getLength();
    buffer.position(metadataLength);
    record = new PersistedIndexedRecord(new UnsafeBuffer(buffer.slice()));
  }

  public int getMetadataLength() {
    return metadata.getLength();
  }

  public int getIndexedRecordLength() {
    return record.getLength();
  }

  @Override
  public long index() {
    return record.index();
  }

  @Override
  public long asqn() {
    return record.asqn();
  }

  @Override
  public long checksum() {
    return metadata.checksum();
  }

  @Override
  public DirectBuffer data() {
    return record.data();
  }

  public long computeChecksum() {
    return -1;
  }

  public int getLength() {
    return metadata.getLength() + record.getLength();
  }
}
