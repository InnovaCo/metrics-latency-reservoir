package eu.inn.metrics.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Sink<T> {

    private final int sinkSize;

    private final LinkedBlockingQueue<Option<T>> sink;

    private final AtomicInteger nonEmptyElementsCount = new AtomicInteger(0);

    public Sink(int sinkSize) {
        this.sinkSize = sinkSize;
        this.sink = new LinkedBlockingQueue<>(sinkSize);
    }

    public boolean add(T element) {
        Option<T> option = Option.create(element);
        boolean needInsertElement = !queueFullOfEmptyElements() || option.isDefined();
        if (needInsertElement) {
            return insertAndSlide(option);
        }
        return false;
    }

    public List<T> getAll() {
        Option<T>[] options = sink.toArray(new Option[0]);
        ArrayList<T> list = new ArrayList<>(options.length);
        for (Option<T> option: options) {
            if (option.isDefined()) {
                list.add(option.get());
            }
        }
        return list;
    }

    private boolean queueFullOfEmptyElements() {
        return sink.size() == sinkSize && nonEmptyElementsCount.get() == 0;
    }

    private boolean insertAndSlide(Option<T> option) {
        if (sink.size() == sinkSize) {
            Option<T> polled = sink.poll();
            if (polled.isDefined()) {
                nonEmptyElementsCount.decrementAndGet();
            }
        }
        if (option.isDefined()) {
            nonEmptyElementsCount.incrementAndGet();
        }
        return sink.add(option);
    }
}

abstract class Option<T> {

    abstract boolean isDefined();

    abstract T get();

    private final static Option None = new Option() {
        @Override
        boolean isDefined() {
            return false;
        }

        @Override
        Object get() {
            return null;
        }
    };

    static <T> Option<T> create(final T value) {
        if (value == null) {
            return None;
        } else {
            return new Option<T>() {
                @Override
                boolean isDefined() {
                    return true;
                }

                @Override
                T get() {
                    return value;
                }
            };
        }
    }
}