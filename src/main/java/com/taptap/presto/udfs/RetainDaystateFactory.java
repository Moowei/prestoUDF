package com.taptap.presto.udfs;

import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

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

        public static final int INSTANCE_SIZE = ClassLayout.parseClass(RetainDayState.class).instanceSize();

        private long size;


        public void ensureCapacity(long size) {
            retainDayArray.ensureCapacity(size);
        }

        public List<Integer> getRetainArray() {
            return retainDayArray.get(getGroupId());
        }

        public void setRetainArray(List<Integer> values) {
            retainDayArray.set(getGroupId(), values);
        }

        public void addMemoryUsage(int value) {
            size += value;
        }

        public long getEstimatedSize() {
            return INSTANCE_SIZE + retainDayArray.sizeOf();
        }
    }


    /**
     * 如果是单个数组, 这个是在单台机器上
     */
    public static class SingleArrayAggregationState implements RetainDayState {

        private List<Integer> retainDayArrays;

        public static final int INSTANCE_SIZE = ClassLayout.parseClass(RetainDayState.class).instanceSize();

        public List<Integer> getRetainArray() {
            return retainDayArrays;
        }

        public void setRetainArray(List<Integer> retainArrayValues) {
            this.retainDayArrays = retainArrayValues;
        }

        public void addMemoryUsage(int value) {
            //这里先进行忽略
        }

        public long getEstimatedSize() {
            long estimatedSize = INSTANCE_SIZE;
            if (retainDayArrays != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(retainDayArrays.size());
            }
            return estimatedSize;
        }
    }
}