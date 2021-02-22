/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl.log;

import static io.zeebe.util.StringUtil.getBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.util.LogStreamReaderRule;
import io.zeebe.logstreams.util.LogStreamRule;
import io.zeebe.logstreams.util.LogStreamWriterRule;
import io.zeebe.util.ByteValue;
import java.util.NoSuchElementException;
import java.util.Random;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public final class LogStreamReaderTest {
  private static final UnsafeBuffer EVENT_VALUE = new UnsafeBuffer(getBytes("test"));
  private static final int LOG_SEGMENT_SIZE = (int) ByteValue.ofMegabytes(4);

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final LogStreamRule logStreamRule =
      LogStreamRule.startByDefault(
          temporaryFolder,
          builder -> builder.withMaxFragmentSize(LOG_SEGMENT_SIZE),
          builder -> builder.withMaxEntrySize(LOG_SEGMENT_SIZE));
  private final LogStreamWriterRule writer = new LogStreamWriterRule(logStreamRule);
  private final LogStreamReaderRule readerRule = new LogStreamReaderRule(logStreamRule);

  @Rule
  public final RuleChain ruleChain =
      RuleChain.outerRule(temporaryFolder).around(logStreamRule).around(readerRule).around(writer);

  private final Random random = new Random();

  private LogStreamReader reader;
  private long eventKey;

  @Before
  public void setUp() {
    eventKey = random.nextLong();
    reader = readerRule.getLogStreamReader();
  }

  @Test
  public void shouldNotHaveNextIfReaderIsClosed() {
    // given
    final LogStreamReader reader = logStreamRule.getLogStreamReader();
    reader.close();

    // when - then
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldThrowExceptionIfReaderClosedOnNext() {
    // given
    final LogStreamReader reader = logStreamRule.getLogStreamReader();
    reader.close();

    // when - then
    assertThatCode(reader::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldNotHaveNext() {
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldHaveNext() {
    // given
    final long position = writer.writeEvent(w -> w.key(eventKey).value(EVENT_VALUE));

    // then
    assertThat(reader.hasNext()).isTrue();
    final LoggedEvent next = reader.next();
    assertThat(next.getKey()).isEqualTo(eventKey);
    assertThat(next.getPosition()).isEqualTo(position);
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldThrowNoSuchElementExceptionOnNextCall() {
    // given an empty log
    // then
    assertThatCode(reader::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void shouldReturnPositionOfCurrentLoggedEvent() {
    // given
    final long position = writer.writeEvent(EVENT_VALUE);
    reader.seekToFirstEvent();

    // then
    assertThat(reader.getPosition()).isEqualTo(position);
  }

  @Test
  public void shouldReturnNoPositionIfNotActiveOrInitialized() {
    // given
    writer.writeEvent(EVENT_VALUE);

    // then
    assertThat(reader.getPosition()).isEqualTo(-1);
  }

  @Test
  public void shouldReopenAndReturnLoggedEvent() {
    // given
    reader.close();
    final long position = writer.writeEvent(w -> w.key(eventKey).value(EVENT_VALUE));
    reader = readerRule.resetReader();

    // then
    final LoggedEvent loggedEvent = readerRule.nextEvent();
    assertThat(loggedEvent.getKey()).isEqualTo(eventKey);
    assertThat(loggedEvent.getPosition()).isEqualTo(position);
  }

  @Test
  public void shouldWrapAndSeekToEvent() {
    // given
    writer.writeEvent(EVENT_VALUE);
    final long secondPos = writer.writeEvent(w -> w.key(eventKey).value(EVENT_VALUE));

    // when
    reader = logStreamRule.newLogStreamReader();
    reader.seek(secondPos);

    // then
    final LoggedEvent loggedEvent = reader.next();
    assertThat(loggedEvent.getKey()).isEqualTo(eventKey);
    assertThat(loggedEvent.getPosition()).isEqualTo(secondPos);

    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldReturnLastEventAfterSeekToLastEvent() {
    // given
    final int eventCount = 10;
    final long lastPosition = writer.writeEvents(eventCount, EVENT_VALUE);

    // when
    final long seekedPosition = reader.seekToEnd();

    // then
    assertThat(reader.hasNext()).isFalse();
    assertThat(lastPosition).isEqualTo(seekedPosition);
  }

  @Test
  public void shouldReturnNextAfterSeekToEnd() {
    // given
    final int eventCount = 10;
    final long lastEventPosition = writer.writeEvents(eventCount, EVENT_VALUE);
    final long seekedPosition = reader.seekToEnd();

    // when
    final long newLastPosition = writer.writeEvent(EVENT_VALUE);

    // then
    assertThat(lastEventPosition).isEqualTo(seekedPosition);
    assertThat(newLastPosition).isGreaterThan(seekedPosition);

    assertThat(reader.hasNext()).isTrue();
    final LoggedEvent loggedEvent = reader.next();
    assertThat(loggedEvent.getPosition()).isEqualTo(newLastPosition);
  }

  @Test
  public void shouldSeekToEnd() {
    // given
    final int eventCount = 1000;
    final long lastPosition = writer.writeEvents(eventCount, EVENT_VALUE);

    // when
    final long seekedPosition = reader.seekToEnd();

    // then
    assertThat(lastPosition).isEqualTo(seekedPosition);
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldIterateOverManyEvents() {
    // given
    final int eventCount = 100_000;

    // when
    writer.writeEvents(eventCount, EVENT_VALUE);

    // then
    readerRule.assertEvents(eventCount, EVENT_VALUE);
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldIterateMultipleTimes() {
    // given
    final int eventCount = 500;
    writer.writeEvents(eventCount, EVENT_VALUE);

    // when
    reader.seekToFirstEvent();
    readerRule.assertEvents(eventCount, EVENT_VALUE);
    assertThat(reader.hasNext()).isFalse();

    reader.seekToFirstEvent();
    readerRule.assertEvents(eventCount, EVENT_VALUE);
    assertThat(reader.hasNext()).isFalse();

    reader.seekToFirstEvent();
    readerRule.assertEvents(eventCount, EVENT_VALUE);
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldSeekToFirstEvent() {
    // given
    final long firstPosition = writer.writeEvent(EVENT_VALUE);
    writer.writeEvents(2, EVENT_VALUE);

    // when
    reader.seekToFirstEvent();

    // then
    assertThat(reader.hasNext()).isTrue();
    assertThat(reader.next().getPosition()).isEqualTo(firstPosition);
  }

  @Test
  public void shouldSeekToFirstPositionWhenPositionBeforeFirstEvent() {
    // given
    final long firstPosition = writer.writeEvent(EVENT_VALUE);
    writer.writeEvents(2, EVENT_VALUE);

    // when
    reader.seek(firstPosition - 1);

    // then
    assertThat(reader.hasNext()).isTrue();
    assertThat(reader.next().getPosition()).isEqualTo(firstPosition);
  }

  @Test
  public void shouldNotSeekToEventBeyondLastEvent() {
    // given
    final long lastEventPosition = writer.writeEvents(100, EVENT_VALUE);

    // when
    reader.seek(lastEventPosition + 1);

    // then
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldReturnNegativeOnSeekToEndOfEmptyLog() {
    // given
    final var reader = logStreamRule.getLogStreamReader();

    // when
    final var result = reader.seekToEnd();

    // then
    assertThat(result).isNegative();
  }

  @Test
  public void shouldSeekToNextEventWhenThereIsNone() {
    // given
    final long lastEventPosition = writer.writeEvents(10, EVENT_VALUE);

    // when
    final boolean positionExists = reader.seekToNextEvent(lastEventPosition);

    // then
    assertThat(reader.hasNext()).isFalse();
    assertThat(positionExists).isTrue();
    assertThat(reader.getPosition()).isEqualTo(lastEventPosition);
  }

  @Test
  public void shouldSeekToNextEvent() {
    // given
    final long lastEventPosition = writer.writeEvents(10, EVENT_VALUE);

    // when
    final boolean positionExists = reader.seekToNextEvent(lastEventPosition - 1);

    // then
    assertThat(positionExists).isTrue();
    assertThat(reader).hasNext();
    assertThat(reader.next().getPosition()).isEqualTo(lastEventPosition);
  }

  @Test
  public void shouldNotSeekToNextEvent() {
    // given
    final long lastEventPosition = writer.writeEvents(10, EVENT_VALUE);

    // when
    final boolean positionExists = reader.seekToNextEvent(lastEventPosition + 1);

    // then
    assertThat(positionExists).isFalse();
    assertThat(reader.hasNext()).isFalse();
  }

  @Test
  public void shouldSeekToFirstEventWhenNextIsNegative() {
    // given
    final long firstEventPosition = writer.writeEvent(EVENT_VALUE);
    writer.writeEvents(10, EVENT_VALUE);
    reader.seekToEnd();

    // when
    final boolean positionExists = reader.seekToNextEvent(-1);

    // then
    assertThat(positionExists).isTrue();
    assertThat(reader).hasNext();
    assertThat(reader.next().getPosition()).isEqualTo(firstEventPosition);
  }
}
