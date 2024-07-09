package org.apache.hudi.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * @author zhouyu
 * @date 2024/7/9
 */
public class BulkSinkRuntimeProviderContext implements DynamicTableSink.Context{

    private final SinkFunction<Object> sinkFunction;

    public BulkSinkRuntimeProviderContext(final SinkFunction<Object> sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
        return null;
    }

    public TypeInformation<?> createTypeInformation(LogicalType consumedLogicalType) {
        return null;
    }

    public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType consumedDataType) {
        return null;
    }

    public SinkFunction<Object> getSinkFunction() {
        return sinkFunction;
    }
}
