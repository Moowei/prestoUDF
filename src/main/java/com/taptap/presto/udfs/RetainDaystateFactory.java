package com.taptap.presto.udfs;

import com.facebook.presto.spi.function.AccumulatorStateFactory;

import java.util.List;


/**
 * @Description RetainDaystateFactory
 * @Date 2019/12/30
 * @Author wangwei
 */
public class RetainDaystateFactory implements AccumulatorStateFactory<RetainDayState> {

    public RetainDayState createSingleState() {
        return new SingleArrayAggregationState();
    }

    public Class<? extends RetainDayState> getSingleStateClass() {
        return SingleArrayAggregationState.class;
    }

    public RetainDayState createGroupedState() {
        return new GroupedArrayAggregationState();
    }

    public Class<? extends RetainDayState> getGroupedStateClass() {
        return GroupedArrayAggregationState.class;
    }

    /**
     * 继承抽象的Group计算的类 , 这个是用于做分布式计算的
     */
    public static class GroupedArrayAggregationState extends AbstractGroupedAccumulatorState implements RetainDayState {

        private final ObjectBigArray<List<Integer>> retainDayArray = new ObjectBigArray();

        private final int ARRAY_SIZE = 30;


        public void ensureCapacity(long size) {
            retainDayArray.ensureCapacity(ARRAY_SIZE);
        }

        public List<Integer> getRetainArray() {
            return retainDayArray.get(getGroupId());
        }

        public void setRetainArray(List<Integer> values) {
            retainDayArray.set(getGroupId(), values);
        }

        public long getEstimatedSize() {
            return ARRAY_SIZE;
        }
    }


    /**
     * 如果是单个数组, 这个是在单台机器上
     */
    public static class SingleArrayAggregationState implements RetainDayState {

        private List<Integer> retainDayArrays;
        private final int ARRAY_SIZE = 30;

        public List<Integer> getRetainArray() {
            return retainDayArrays;
        }

        public void setRetainArray(List<Integer> retainArrayValues) {
            this.retainDayArrays = retainArrayValues;
        }

        public long getEstimatedSize() {
            return ARRAY_SIZE;
        }
    }
}