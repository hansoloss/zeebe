package io.zeebe.journal.file.record;

import io.zeebe.journal.JournalRecord;
import java.nio.ByteBuffer;

public class SBEJournalRecordReader implements JournalRecordBufferReader {

  @Override
  public JournalRecord read(final ByteBuffer buffer) {
    return null;
  }
}
