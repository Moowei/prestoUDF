package com.taptap.presto.udfs;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;


import java.util.List;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

/**
 * @Description RetainDaystateSerializer
 * @Date 2019/12/30
 * @Author wangwei
 */
public class RetainDaystateSerializer implements AccumulatorStateSerializer<RetainDayState> {

    public Type getSerializedType() {
        return VARBINARY;
    }

    /**
     * 序列化
     *
     * @param state
     * @param out
     */
    public void serialize(RetainDayState state, BlockBuilder out) {
        //TODO 第二版开发
//        Integer[] retainArray = state.getRetainArray();
//        for (int i = 0; i < retainArray.length; i++) {
//            out.writeInt(retainArray[i]);
//        }
//        out.closeEntry();

        //TODO  第三版开发
        if (state.getRetainArray() == null) {
            out.appendNull();
        }
        List<Integer> retainArrayList = state.getRetainArray();
        SliceOutput output = Slices.allocate(retainArrayList.size()).getOutput();
        // 这里用Long去读取大小
        output.appendLong(retainArrayList.size());
        for (Integer resultValue : retainArrayList) {
            output.appendInt(resultValue);
        }
        VARBINARY.writeSlice(out, output.slice());

    }

    /**
     * 反序列
     *
     * @param block
     * @param index
     * @param state
     */
    public void deserialize(Block block, int index, RetainDayState state) {
        //TODO 第二版开发
//        int entriesSize = state.getEntries();
//        Integer[] retainArray = state.getRetainArray();
//        for (int i = 0; i < entriesSize; i++) {
//            retainArray[i] = block.getInt(i);
//        }
//        state.setRetainArray(retainArray);

        //TODO 第三版开发
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        long resultListSize = input.readLong();
        //这里是构建一个结果对象
        ImmutableList.Builder<Integer> retainDyaListBuilder = ImmutableList.builder();
        for (int i = 0; i < resultListSize; i++) {
            retainDyaListBuilder.add(input.readInt());
        }
        state.setRetainArray(retainDyaListBuilder.build());


    }
}