package com.taptap.presto.udfs;
import com.facebook.presto.spi.function.GroupedAccumulatorState;

/**
 * @author
 */
public abstract class AbstractGroupedAccumulatorState
        implements GroupedAccumulatorState
{
    private long groupId;

    public final void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }

    protected final long getGroupId()
    {
        return groupId;
    }
}