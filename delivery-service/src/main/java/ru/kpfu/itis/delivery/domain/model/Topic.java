package ru.kpfu.itis.delivery.domain.model;

import org.apache.kafka.common.serialization.Serde;

public class Topic<K, V> {
    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public Topic(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public String name() {
        return name;
    }

    public String toString() {
        return name;
    }
}
