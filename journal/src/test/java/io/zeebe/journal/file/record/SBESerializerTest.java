package io.zeebe.journal.file.record;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.journal.file.ChecksumGenerator;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SBESerializerTest {

  private PersistableJournalRecord firstRecord;
  private PersistableJournalRecord secondRecord;

  @BeforeEach
  public void setup() {
    final DirectBuffer data = new UnsafeBuffer();
    data.wrap("Test".getBytes());
    firstRecord = new PersistableJournalRecord(1, 2, data);
    secondRecord = new PersistableJournalRecord(2, 4, data);
  }

  @Test
  public void shouldWriteRecord() {
    // given
    final ByteBuffer buffer = ByteBuffer.allocate(firstRecord.getLength());
    final SBESerializer serializer = new SBESerializer(new ChecksumGenerator());

    // when
    final var recordWritten = serializer.write(firstRecord, buffer);

    // then
    assertThat(recordWritten.index()).isEqualTo(1);
    assertThat(recordWritten.asqn()).isEqualTo(2);
  }

  @Test
  public void shouldReadRecord() {
    // given
    final ByteBuffer buffer = ByteBuffer.allocate(firstRecord.getLength());
    final SBESerializer serializer = new SBESerializer(new ChecksumGenerator());
    final var recordWritten = serializer.write(firstRecord, buffer);

    // when
    buffer.position(0);
    final var recordRead = serializer.read(buffer);

    // then
    assertThat(recordRead).isEqualTo(recordWritten);
  }

  @Test
  public void shouldAdvancePositionAfterWritingAndReading() {
    // given
    final ByteBuffer buffer = ByteBuffer.allocate(firstRecord.getLength() * 2);
    final SBESerializer serializer = new SBESerializer(new ChecksumGenerator());
    final var firstRecordWritten = serializer.write(firstRecord, buffer);
    assertThat(firstRecordWritten.index()).isEqualTo(1);

    // when
    final var secondRecordWritten = serializer.write(secondRecord, buffer);
    assertThat(secondRecordWritten.index()).isEqualTo(2);

    // then
    buffer.position(0);
    final var recordRead = serializer.read(buffer);
    assertThat(recordRead).isEqualTo(firstRecordWritten);

    final var recordRead2 = serializer.read(buffer);
    assertThat(recordRead2).isEqualTo(secondRecordWritten);
  }

  @Test
  public void shouldRightAndReadRecordAtAnyPosition() {
    // given
    final int initialOffset = 8;
    final ByteBuffer buffer = ByteBuffer.allocate(initialOffset + firstRecord.getLength() * 2);
    final SBESerializer serializer = new SBESerializer(new ChecksumGenerator());

    // when
    buffer.position(initialOffset); // should start writing from a non-zero start position

    final var firstRecordWritten = serializer.write(firstRecord, buffer);
    assertThat(firstRecordWritten.index()).isEqualTo(1);

    final var secondRecordWritten = serializer.write(secondRecord, buffer);
    assertThat(secondRecordWritten.index()).isEqualTo(2);

    // then
    buffer.position(initialOffset);
    final var recordRead = serializer.read(buffer);
    assertThat(recordRead).isEqualTo(firstRecordWritten);

    final var recordRead2 = serializer.read(buffer);
    assertThat(recordRead2).isEqualTo(secondRecordWritten);
  }
}
