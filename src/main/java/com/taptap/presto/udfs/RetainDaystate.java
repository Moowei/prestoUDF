package com.taptap.presto.udfs;


import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.slice.SliceOutput;


@AccumulatorStateMetadata(stateFactoryClass = RetainDaystateFactory.class, stateSerializerClass = RetainDaystateSerializer.class)
public interface RetainDaystate extends AccumulatorState {
    /**
     * 获取到时间间隔差的自定义变量
     *
     * @return
     */
    Integer[] getRetainArray();

    void setRetainArray(Integer[] retinArray);

    int getEntries();

    void setEntries(int entries);


    /**
     * 二次开发
     * @return
     */
    SliceOutput getSliceOutput();
    void setSliceOutput(SliceOutput value);


}
