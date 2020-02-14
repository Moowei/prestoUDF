package com.taptap.presto.udfs;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class UdfPlugin  implements Plugin {
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(BitmapAggregation.class)
                .build();
    }
}
