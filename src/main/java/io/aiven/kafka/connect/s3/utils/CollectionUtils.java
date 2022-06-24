package io.aiven.kafka.connect.s3.utils;

import java.util.List;
import java.util.Optional;

public final class CollectionUtils {
    public static <T> Optional<T> last(List<T> list) {
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(list.size() - 1));
    }
}
