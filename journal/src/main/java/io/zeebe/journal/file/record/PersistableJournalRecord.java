package io.zeebe.journal.file.record;

import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class PersistableJournalRecord {

  final long index;
  final long asqn;
  final DirectBuffer data;
  final PersistableJournalRecordMetadata metadata = new PersistableJournalRecordMetadata();
  final PersistableIndexedRecord record ;

  public PersistableJournalRecord(final long index, final long asqn, final DirectBuffer data) {
    this.index = index;
    this.asqn = asqn;
    this.data = data;
    record = new PersistableIndexedRecord(index, asqn, data);
  }

  public void write(final MutableDirectBuffer buffer) {
    metadata.write(buffer, 0);
    record.write(buffer, metadata.getLength());
    metadata.setChecksum(computeChecksum(buffer, metadata.getLength(), record.getLength()));
    metadata.write(buffer, 0);
  }

  private long computeChecksum(final DirectBuffer data, final int offset, final int length) {
    final byte[] slice = new byte[length];
    data.getBytes(offset, slice);
    final CRC32C crc32 = new CRC32C();
    crc32.reset();
    crc32.update(slice);
    return crc32.getValue();
  }

  public int getLength() {
      return metadata.getLength() + record.getLength();
    }
}
