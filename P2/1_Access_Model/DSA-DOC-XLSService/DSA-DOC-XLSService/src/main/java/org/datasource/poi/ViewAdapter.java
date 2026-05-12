package org.datasource.poi;

import java.util.Map;

/* The Adapter to convert the Map-Tuple into <V> JavaBean instances has to be provided */
@FunctionalInterface
public interface ViewAdapter<V> {
    V map(Map<String, Object> tuple);
}
