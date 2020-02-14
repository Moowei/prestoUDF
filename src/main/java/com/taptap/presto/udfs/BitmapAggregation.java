package com.taptap.presto.udfs;


import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.*;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;


@AggregationFunction("calcRetain")
public class BitmapAggregation {
    private BitmapAggregation() {

    }

    //TODO
    // select  user.identify, user.identifyTime, group_user_to_bitmap(user.identifyTime , event.identify)
    // From event
    // Join user
    // on user.identify = event.identify
    // Group by  user.identify,user.identifyTime
//    String test =  " Select calcRetain(user.identifyTime , event.inserttime) From event Join user  on user.identify = event.identify Group by  user.identify, user.identifytime Where event.name = 'HAU' AND event.inserttime  ";

//    Select calcRetain(user.identifyTime , event.inserttime) From event Join user  on user.identify = event.identify
//    Where event.name = 'HAU'
//    AND user.identifyTime   BETWEEN timestamp  '2019-10-23 00:00:00.000' And timestamp '2019-10-28 23:59:59.000'
//    AND  event.inserttime  BETWEEN timestamp  '2019-10-23 00:00:00.000' And timestamp '2019-11-05 23:59:59.000'
//    Group by user.identify, user.identifytime ;

    /**
     * 表示函数的输入
     *
     * @return
     */
    @InputFunction
    public static void input(@AggregationState RetainDaystate state, @SqlType(StandardTypes.TIMESTAMP) long userTime, @SqlType(StandardTypes.TIMESTAMP) long eventTime) {

        SliceOutput sliceOutput = state.getSliceOutput();
        //TODO 第一版
        //数组中的下标+1 就是多少天的次日留存
        Integer[] retainArrayResult = state.getRetainArray();
        long timediff = eventTime - userTime;
        long hour = timediff / (1000 * 60 * 60);
        //数组中的下标+1 就是多少天的次日留存
        retainArrayResult[0] = (hour > 0 && hour <= 24) ? 1 : 0;
        retainArrayResult[1] = (hour > 24 && hour <= 48) ? 1 : 0;
        retainArrayResult[2] = (hour > 48 && hour <= 72) ? 1 : 0;
        retainArrayResult[3] = (hour > 72 && hour <= 96) ? 1 : 0;
        retainArrayResult[4] = (hour > 96 && hour <= 128) ? 1 : 0;
        retainArrayResult[5] = (hour > 128 && hour <= 152) ? 1 : 0;
        for (int i = 0; i < retainArrayResult.length; i++) {
            sliceOutput.appendInt(retainArrayResult[i]);
        }
        state.setSliceOutput(sliceOutput);
    }


    /**
     * 不同节点的结果合并
     */
    @CombineFunction
    public static void combine(@AggregationState RetainDaystate state, @AggregationState RetainDaystate otherState) {
        SliceOutput sliceOutput = state.getSliceOutput();
        SliceOutput otherSliceOutput = otherState.getSliceOutput();
        if (sliceOutput == null && otherSliceOutput != null){
            state.setSliceOutput(otherSliceOutput);
        }else if (sliceOutput != null){
            sliceOutput.appendBytes(otherSliceOutput.slice());
            state.setEntries(state.getEntries()+otherState.getEntries());
        }
    }

    /**
     * 函数最后的输出
     */
    @OutputFunction("array(integer)")
    public static void output(@AggregationState RetainDaystate state, BlockBuilder out) {

        List<Integer> resultList = new ArrayList<Integer>(state.getEntries());
        if (state.getSliceOutput() == null){
            out.appendNull();
        } else {
            SliceInput sliceInput = state.getSliceOutput().slice().getInput();
            long entries = state.getEntries();
            for (int i = 0; i < entries; i++) {
                Integer value = new Integer(sliceInput.readInt());
                resultList.add(value);
            }
            BlockBuilderStatus blockBuilderStatus = new PageBuilderStatus(DEFAULT_MAX_PAGE_SIZE_IN_BYTES).createBlockBuilderStatus();
            BlockBuilder blockBuilder = IntegerType.INTEGER.createBlockBuilder(blockBuilderStatus, resultList.size());
            for (Integer integer : resultList) {
                BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
                subBlockBuilder.writeInt(integer);
            }
            blockBuilder.build();
            blockBuilder.closeEntry();
        }


    }


}

