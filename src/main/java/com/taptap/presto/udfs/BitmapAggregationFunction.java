package com.taptap.presto.udfs;


import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.*;

import java.util.ArrayList;
import java.util.List;


@AggregationFunction("calcRetain")
public class BitmapAggregationFunction {
    private BitmapAggregationFunction() {

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
//    AND user.identifyTime   BETWEEN timestamp  '2019-10-23 00:00:00.000' And timestamp '2019-10-26 23:59:59.000'
//    AND  event.inserttime  BETWEEN timestamp  '2019-10-23 00:00:00.000' And timestamp '2019-11-10 23:59:59.000'
//    Group by user.identify, user.identifytime ;

    /**
     * 表示函数的输入
     *
     * @return
     */
    @InputFunction
    public static void input(@AggregationState RetainDayState state, @SqlType(StandardTypes.TIMESTAMP) long userTime, @SqlType(StandardTypes.TIMESTAMP) long eventTime) {

        //TODO 第二版的开发
//        SliceOutput sliceOutput = state.getSliceOutput();
//        //数组中的下标+1 就是多少天的次日留存
//        Integer[] retainArrayResult = state.getRetainArray();
//        long timediff = eventTime - userTime;
//        long hour = timediff / (1000 * 60 * 60);
//        //数组中的下标+1 就是多少天的次日留存
//        retainArrayResult[0] = (hour > 0 && hour <= 24) ? 1 : 0;
//        retainArrayResult[1] = (hour > 24 && hour <= 48) ? 1 : 0;
//        retainArrayResult[2] = (hour > 48 && hour <= 72) ? 1 : 0;
//        retainArrayResult[3] = (hour > 72 && hour <= 96) ? 1 : 0;
//        retainArrayResult[4] = (hour > 96 && hour <= 128) ? 1 : 0;
//        retainArrayResult[5] = (hour > 128 && hour <= 152) ? 1 : 0;
//        for (int i = 0; i < retainArrayResult.length; i++) {
//            sliceOutput.appendInt(retainArrayResult[i]);
//        }
//        state.setSliceOutput(sliceOutput);

        //TODO 第三版的开发
        List<Integer> retainArrayResult = state.getRetainArray();
        long timediff = eventTime - userTime;
        long hour = timediff / (1000 * 60 * 60);
        if (hour > 0 && hour <= 24) {
            retainArrayResult.set(0, 1);
        } else if (hour > 24 && hour <= 48) {
            retainArrayResult.set(1, 1);
        } else if (hour > 48 && hour <= 72) {
            retainArrayResult.set(2, 1);
        } else if (hour > 72 && hour <= 96) {
            retainArrayResult.set(3, 1);
        } else if (hour > 96 && hour <= 128) {
            retainArrayResult.set(4, 1);
        } else if (hour > 128 && hour <= 152) {
            retainArrayResult.set(5, 1);
        }


    }


    /**
     * 不同节点的结果合并
     */
    @CombineFunction
    public static void combine(@AggregationState RetainDayState state, @AggregationState RetainDayState otherState) {

        //TODO 第二次的开发
//        SliceOutput sliceOutput = state.getSliceOutput();
//        SliceOutput otherSliceOutput = otherState.getSliceOutput();
//        if (sliceOutput == null && otherSliceOutput != null){
//            state.setSliceOutput(otherSliceOutput);
//        }else if (sliceOutput != null){
//            sliceOutput.appendBytes(otherSliceOutput.slice());
//            state.setEntries(state.getEntries()+otherState.getEntries());
//        }

        //TODO 第三次的开发
        List<Integer> retainArrayList = otherState.getRetainArray();
        for (int i = 0; i < retainArrayList.size(); i++) {
            state.getRetainArray().set(i, retainArrayList.get(i));
        }

    }

    /**
     * 函数最后的输出
     */
    @OutputFunction("array(integer)")
    public static void output(@AggregationState RetainDayState state, BlockBuilder out) {

        //TODO 第二次的开发
//        List<Integer> resultList = new ArrayList<Integer>(state.getEntries());
//        if (state.getSliceOutput() == null){
//            out.appendNull();
//        } else {
//            SliceInput sliceInput = state.getSliceOutput().slice().getInput();
//            long entries = state.getEntries();
//            for (int i = 0; i < entries; i++) {
//                Integer value = new Integer(sliceInput.readInt());
//                resultList.add(value);
//            }
//            BlockBuilderStatus blockBuilderStatus = new PageBuilderStatus(DEFAULT_MAX_PAGE_SIZE_IN_BYTES).createBlockBuilderStatus();
//            BlockBuilder blockBuilder = IntegerType.INTEGER.createBlockBuilder(blockBuilderStatus, resultList.size());
//            for (Integer integer : resultList) {
//                BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
//                subBlockBuilder.writeInt(integer);
//            }
//            blockBuilder.build();
//            blockBuilder.closeEntry();
//        }

        //TODO 开发第三版
        List<Integer> resultList = new ArrayList<Integer>(state.getRetainArray().size());
        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        if (state.getRetainArray() == null) {
            out.appendNull();
        } else {
            for (int i = 0; i < resultList.size(); i++) {
                Integer resultValue = resultList.get(i);
                IntegerType.INTEGER.writeLong(arrayBlockBuilder, resultValue);
            }
        }
        out.closeEntry();
    }

}

