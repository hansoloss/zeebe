/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.journal.file.record;

import io.zeebe.journal.JournalRecord;
import io.zeebe.journal.file.ChecksumGenerator;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class SBESerializer implements JournalRecordBufferWriter, JournalRecordBufferReader {

  private final ChecksumGenerator checksumGenerator;

  public SBESerializer(final ChecksumGenerator checksumGenerator) {
    this.checksumGenerator = checksumGenerator;
  }

  @Override
  public JournalRecord read(final ByteBuffer buffer) {
    final var recordPosition = buffer.position();
    try {
      final var record = new PersistedJournalRecord(buffer);
      buffer.position(recordPosition + record.getMetadataLength());
      final var expectedChecksum = computeChecksum(buffer, record.getIndexedRecordLength());
      if (record.checksum() != expectedChecksum) {
        buffer.position(recordPosition);
        return null; // TODO: throw an exception??
      }
      buffer.position(recordPosition + record.getLength());
      return record;
    } catch (final Exception e) {
      buffer.position(recordPosition);
      return null;
    }
  }

  @Override
  public JournalRecord write(final JournalRecord record, final ByteBuffer buffer) {
    final int recordStartPosition = buffer.position();
    buffer.mark();

    final PersistableIndexedRecord indexedRecord =
        new PersistableIndexedRecord(record.index(), record.asqn(), record.data());
    final PersistableJournalRecordMetadata recordMetadata = new PersistableJournalRecordMetadata();
    final var recordLength = recordMetadata.getLength() + indexedRecord.getLength();
    if (buffer.position() + recordLength > buffer.limit()) {
      throw new BufferOverflowException();
    }

    // If the entry length exceeds the maximum entry size then throw an exception.
    // TODO
    /*if (recordLength > maxEntrySize) {
      // Just reset the buffer. There's no need to zero the bytes since we haven't written the
      // length or checksum.
      buffer.reset();
      throw new StorageException.TooLarge(
          "Entry size " + recordLength + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
    }*/

    final MutableDirectBuffer bufferToWrite = new UnsafeBuffer();
    buffer.position(recordStartPosition + recordMetadata.getLength());
    bufferToWrite.wrap(buffer, buffer.position(), indexedRecord.getLength());
    indexedRecord.write(bufferToWrite, 0);
    buffer.position(recordStartPosition + recordMetadata.getLength());
    final long checksum = computeChecksum(buffer, indexedRecord.getLength());
    recordMetadata.setChecksum(checksum);
    buffer.position(recordStartPosition);
    bufferToWrite.wrap(buffer, recordStartPosition, recordLength);
    recordMetadata.write(bufferToWrite, 0);

    buffer.position(recordStartPosition);
    final var recordWritten = new PersistedJournalRecord(buffer);
    buffer.position(recordStartPosition + recordLength);
    return recordWritten;
  }

  private long computeChecksum(final ByteBuffer buffer, final int length) {
    final var record = buffer.slice();
    record.limit(length);
    return checksumGenerator.compute(record);
  }
}
