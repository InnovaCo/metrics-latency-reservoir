package eu.inn.metrics;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.AssertJUnit.*;

public class SinkTest {

    @Test
    public void storeAllNonEmptyElementsBeforeSlide() {
        Sink<Integer> sink = new Sink<>(3);

        List<Integer> tracks = Arrays.asList(1, 2, 3);

        for (Integer t: tracks) {
            sink.add(t);
        }

        assertEquals(tracks, sink.getAll());
    }

    @Test
    public void slideWithNonEmptyElements() {
        Sink<Integer> sink = new Sink<>(3);

        List<Integer> tracks = Arrays.asList(1, 2, 3, 4);

        for (Integer t: tracks) {
            sink.add(t);
        }

        assertEquals(Arrays.asList(2, 3, 4), sink.getAll());
    }

    @Test
    public void slideWithEmptyElements() {
        Sink<Integer> sink = new Sink<>(3);

        List<Integer> tracks = Arrays.asList(1, 2, 3);

        for (Integer t: tracks) {
            sink.add(t);
        }

        assertEquals(tracks, sink.getAll());

        for (Integer t: tracks) {
            sink.add(null);
        }

        assertTrue(sink.getAll().isEmpty());
    }

    @Test
    public void doNotSlideFullOfEmptySink() {
        Sink<Integer> sink = new Sink<>(3);

        for (int i = 0; i< 3; i++) {
            assertTrue(sink.add(null));
        }

        assertFalse(sink.add(null));

        assertTrue(sink.getAll().isEmpty());
    }

    @Test
    public void slideFullOfEmptySinkOnNonEmptyElementAdd() {
        Sink<Integer> sink = new Sink<>(3);

        for (int i = 0; i< 3; i++) {
            assertTrue(sink.add(null));
        }

        assertTrue(sink.add(1));

        assertEquals(Arrays.asList(1), sink.getAll());
    }
}
