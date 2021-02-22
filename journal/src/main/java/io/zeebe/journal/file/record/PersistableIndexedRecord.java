package io.zeebe.journal.file.record;

import io.zeebe.journal.file.JournalIndexedRecordEncoder;
import io.zeebe.journal.file.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class PersistableIndexedRecord {

  protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final JournalIndexedRecordEncoder encoder = new JournalIndexedRecordEncoder();
  private final long index;
  private final long asqn;
  private final DirectBuffer data;

  public PersistableIndexedRecord(final long index, final long asqn, final DirectBuffer data) {
    this.index = index;
    this.asqn = asqn;
    this.data = data;
  }

  /**
   * Returns the length required to write this record to a buffer
   *
   * @return the length
   */
  public int getLength() {
    return headerEncoder.encodedLength()
        + encoder.sbeBlockLength()
        + JournalIndexedRecordEncoder.contentHeaderLength()
        + data.capacity();
  }

  public void write(final MutableDirectBuffer buffer, final int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(encoder.sbeBlockLength())
        .templateId(encoder.sbeTemplateId())
        .schemaId(encoder.sbeSchemaId())
        .version(encoder.sbeSchemaVersion());

    encoder.wrap(buffer, offset + headerEncoder.encodedLength());

    encoder.index(index).asqn(asqn).putContent(data, 0, data.capacity());
  }
}
