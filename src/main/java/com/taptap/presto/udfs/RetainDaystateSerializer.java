package com.taptap.presto.udfs;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;


import static com.facebook.presto.spi.type.IntegerType.INTEGER;

/**
 * @Description RetainDaystateSerializer
 * @Date 2019/12/30
 * @Author wangwei
 */
public class RetainDaystateSerializer implements AccumulatorStateSerializer<RetainDaystate> {

    public Type getSerializedType() {
        return INTEGER;
    }

    /**
     * 序列化
     *
     * @param state
     * @param out
     */
    public void serialize(RetainDaystate state, BlockBuilder out) {
        Integer[] retainArray = state.getRetainArray();
        for (int i = 0; i < retainArray.length; i++) {
            out.writeInt(retainArray[i]);
        }
        out.closeEntry();
    }

    /**
     * 反序列
     *
     * @param block
     * @param index
     * @param state
     */
    public void deserialize(Block block, int index, RetainDaystate state) {
        int entriesSize = state.getEntries();
        Integer[] retainArray = state.getRetainArray();
        for (int i = 0; i < entriesSize; i++) {
            retainArray[i] = block.getInt(i);
        }
        state.setRetainArray(retainArray);
    }
}