package com.taptap.presto.udfs;

import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.SliceOutput;


/**
 * @Description RetainDaystateFactory
 * @Date 2019/12/30
 * @Author wangwei
 */
public class RetainDaystateFactory implements AccumulatorStateFactory<RetainDaystate> {

    public RetainDaystate createSingleState() {
        return new SingleArrayAggregationState();
    }

    public Class<? extends RetainDaystate> getSingleStateClass() {
        return SingleArrayAggregationState.class;
    }

    public RetainDaystate createGroupedState() {
        return new GroupedArrayAggregationState(6);
    }

    public Class<? extends RetainDaystate> getGroupedStateClass() {
        return GroupedArrayAggregationState.class;
    }

    /**
     * 继承抽象的Group计算的类
     */
    public static class GroupedArrayAggregationState extends AbstractGroupedAccumulatorState implements RetainDaystate {

        private final ObjectBigArray<Integer> integerObjectBigArray = new ObjectBigArray<Integer>();

        private int entries = 6;

        //第二版的
        private final ObjectBigArray<SliceOutput> slices = new ObjectBigArray<SliceOutput>();
//        private final IntBigArray intBigArrayEntry = new IntBigArray();

        public GroupedArrayAggregationState(int entries) {
            this.entries = entries;
        }


        public void ensureCapacity(long size) {
            integerObjectBigArray.ensureCapacity(size);
        }

        public Integer[] getRetainArray() {
            Integer[] resultArray = null;
            if (integerObjectBigArray.sizeOf() <= 0 ){
                 resultArray = new Integer[6];
            }else {

                for (int i = 0; i < integerObjectBigArray.sizeOf(); i++) {
                    if (integerObjectBigArray.get(getGroupId()) != null){
                        resultArray[i] = integerObjectBigArray.get(getGroupId());
                    }

                }
            }
            return resultArray;

        }

        public void setRetainArray(Integer[] retinArray) {
            for (int i = 0; i < retinArray.length; i++) {
                integerObjectBigArray.set(getGroupId(),retinArray[i]);
            }
        }

        public int getEntries() {
            return entries;
        }

        public void setEntries(int entries) {
            this.entries = entries;
        }

        public long getEstimatedSize() {
            return integerObjectBigArray.sizeOf();
        }

        //第二版分界线
        public SliceOutput getSliceOutput() {
            SliceOutput sliceOutput = slices.get(getGroupId());
            return sliceOutput;
        }

        public void setSliceOutput(SliceOutput value) {
            slices.ensureCapacity(getGroupId());
            slices.set(getGroupId(), value);
        }

    }


    /**
     * 如果是单个数组
     */
    public static class SingleArrayAggregationState implements RetainDaystate {

        private final ObjectBigArray<Integer> integerObjectBigArray = new ObjectBigArray<Integer>();
        private int entries =  6;

        //第二版
        private SliceOutput slice;


        public Integer[] getRetainArray() {
            Integer[] resultArray = null;
            if (integerObjectBigArray.sizeOf() <= 0 ){
                resultArray = new Integer[6];
            }else {
                for (int i = 0; i < integerObjectBigArray.sizeOf(); i++) {
                    if (integerObjectBigArray.get(i) != null){
                        resultArray[i] = integerObjectBigArray.get(i);
                    }

                }
            }
            return resultArray;
        }

        public void setRetainArray(Integer[] retinArray) {
            for (int i = 0; i < retinArray.length; i++) {
                integerObjectBigArray.set(i,retinArray[i]);
            }
        }

        public int getEntries() {
            return entries;
        }

        public void setEntries(int entries) {
            this.entries = entries;
        }

        public long getEstimatedSize() {
            return integerObjectBigArray.sizeOf();
        }

        //第二版开发
        public SliceOutput getSliceOutput() {
            return slice;
        }

        public void setSliceOutput(SliceOutput value) {
            this.slice = value;
        }
    }

}
