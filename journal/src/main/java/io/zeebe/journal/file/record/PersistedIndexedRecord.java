package io.zeebe.journal.file.record;

import io.zeebe.journal.file.JournalIndexedRecordDecoder;
import io.zeebe.journal.file.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

final class PersistedIndexedRecord {
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final JournalIndexedRecordDecoder decoder = new JournalIndexedRecordDecoder();
  private final DirectBuffer data = new UnsafeBuffer();

  public PersistedIndexedRecord(final DirectBuffer buffer) {
    wrap(buffer);
    decoder.wrapContent(data);
  }

  public long index() {
    return decoder.index();
  }

  public long asqn() {
    return decoder.asqn();
  }

  public DirectBuffer data() {
    return data;
  }

  public int getLength() {
    return headerDecoder.encodedLength()
        + headerDecoder.blockLength()
        + JournalIndexedRecordDecoder.contentHeaderLength()
        + data.capacity();
  }

  public void wrap(final DirectBuffer buffer) {
    wrap(buffer, 0, buffer.capacity());
  }

  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    if (!canRead(buffer, 0)) {
      throw new RuntimeException("Cannot read buffer"); // TODO
    }
    headerDecoder.wrap(buffer, offset);
    decoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());
  }

  public boolean canRead(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    return (headerDecoder.schemaId() == decoder.sbeSchemaId()
        && headerDecoder.templateId() == decoder.sbeTemplateId());
  }
}
