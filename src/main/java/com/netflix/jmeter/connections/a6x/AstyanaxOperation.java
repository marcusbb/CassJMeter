package com.netflix.jmeter.connections.a6x;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Pair;
import org.mortbay.log.Log;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.DynamicComposite;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.model.AbstractComposite.Component;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.SystemUtils;

public class AstyanaxOperation implements Operation
{
    private static ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
    
	private AbstractSerializer keySerializer;
    private AbstractSerializer valueSerializer;
    private ColumnFamily<Object, Object> cfs;
    private AbstractSerializer columnSerializer;
    
    private AbstractSerializer<?>[] compositeKeySerializers;
    private AbstractSerializer<?>[] compositeColumnSerializers;
    private AbstractSerializer<?>[] compositeValueSerializers;

    private final String cfName;
    private final boolean isCounter;

    public class AstyanaxResponseData extends ResponseData
    {
        public AstyanaxResponseData(String response, int size, OperationResult<?> result)
        {
            super(response, size, EXECUTED_ON + result != null ? result.getHost().getHostName() : "", result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0);
        }

        public AstyanaxResponseData(String response, int size, OperationResult<?> result, Object key, Object cn, Object value)
        {
            super(response, size, EXECUTED_ON + (result != null ? result.getHost().getHostName() : ""), (result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0), key, cn, value);
        }
        
        public AstyanaxResponseData(String response, int size, OperationResult<?> result, Object key, Map<?, ?> kv)
        {
            super(response, size, (result == null) ? "" : result.getHost().getHostName(), result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0, key, kv);
        }
    }
    
    AstyanaxOperation(String columnName, boolean isCounter)
    {
        this.cfName = columnName;
        this.isCounter = isCounter;
    }

    @Override
    public void serlizers(AbstractSerializer<?> keySerializer, AbstractSerializer<?> columnSerializer, AbstractSerializer<?> valueSerializer)
    {
        this.cfs = new ColumnFamily(cfName, keySerializer, columnSerializer);
        this.keySerializer = keySerializer;
        this.columnSerializer = columnSerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void compositeSerializers(AbstractSerializer<?>[] compositeKeySerializers, AbstractSerializer<?>[] compositeColumnSerializers, AbstractSerializer<?>[] compositeValueSerializers){
    	this.compositeKeySerializers = compositeKeySerializers;
    	this.compositeColumnSerializers = compositeColumnSerializers;
    	this.compositeValueSerializers = compositeValueSerializers;
    }
    
    @Override
    public ResponseData put(Object key, Object colName, Object value) throws OperationException
    {
        MutationBatch m = AstyanaxConnection.instance.keyspace().prepareMutationBatch();
        if (isCounter)
            m.withRow(cfs, key).incrementCounterColumn(colName, (Long) value);
        else
            m.withRow(cfs, key).putColumn(colName, value, valueSerializer, null);
        try
        {
            OperationResult<Void> result = m.execute();
            return new AstyanaxResponseData("", 0, result, key, colName, value);
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData putComposite(String key, String colName, ByteBuffer value) throws OperationException
    {
        try
        {
            SerializerPackage sp = AstyanaxConnection.instance.keyspace().getSerializerPackage(cfName, false);
            // work around
            ByteBuffer rowKey = sp.keyAsByteBuffer(key);
            ByteBuffer column = sp.columnAsByteBuffer(colName);
            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = new ColumnFamily(cfName, ByteBufferSerializer.get(), ByteBufferSerializer.get());
            ColumnMutation mutation = AstyanaxConnection.instance.keyspace().prepareColumnMutation(columnFamily, rowKey, column);
            OperationResult<Void> result;
            if (isCounter)
                result = mutation.incrementCounterColumn(LongSerializer.get().fromByteBuffer(value)).execute();
            else
                result = mutation.putValue(value, null).execute();
            return new AstyanaxResponseData("", 0, result, key, colName, value);
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData batchCompositeMutate(String key, Map<String, ByteBuffer> nv) throws OperationException
    {
        // TODO implement
        return null;
    }

    @Override
    public ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException
    {
        MutationBatch m = AstyanaxConnection.instance.keyspace().prepareMutationBatch();
        ColumnListMutation<Object> cf = m.withRow(cfs, key);
        for (Map.Entry<?, ?> entry : nv.entrySet())
        {
            if (isCounter)
                cf.incrementCounterColumn(entry.getKey(), (Long) entry.getValue());
            else
                cf.putColumn(entry.getKey(), entry.getValue(), valueSerializer, null);
        }
        try
        {
            OperationResult<Void> result = m.execute();
            return new AstyanaxResponseData("", 0, result, key, nv);
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData get(Object rkey, Object colName) throws OperationException
    {
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        OperationResult<Column<Object>> opResult = null;
        try
        {
        	
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rkey).getColumn(colName).execute();
            bytes = opResult.getResult().getRawName().capacity();
            bytes += opResult.getResult().getByteBufferValue().capacity();
            
            response.append(formatResult(opResult.getResult().getByteBufferValue(), valueSerializer, compositeValueSerializers));            
           
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }

        return new AstyanaxResponseData(response.toString(), bytes, opResult, rkey, colName, null);
    }

    @Override
    public ResponseData getComposite(String key, String colName) throws OperationException
    {
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        OperationResult<Column<ByteBuffer>> opResult = null;
        try
        {
            SerializerPackage sp = AstyanaxConnection.instance.keyspace().getSerializerPackage(cfName, false);
            ByteBuffer bbName = sp.columnAsByteBuffer(colName);
            ByteBuffer bbKey = sp.keyAsByteBuffer(key);
            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = new ColumnFamily(cfName, ByteBufferSerializer.get(), ByteBufferSerializer.get());
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(columnFamily).getKey(bbKey).getColumn(bbName).execute();
            bytes = opResult.getResult().getByteBufferValue().capacity();
            bytes += opResult.getResult().getRawName().capacity();
            String value = SystemUtils.convertToString(valueSerializer, opResult.getResult().getByteBufferValue());
            response.append(value);
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new AstyanaxResponseData(response.toString(), bytes, opResult, key, colName, null);
    }

    @Override
    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        int bytes = 0;
        OperationResult<ColumnList<Object>> opResult = null;
        StringBuffer response = new StringBuffer().append("\n");
        try
        {
        	
        	ByteBufferRange range = buildRange(startColumn, endColumn, reversed, count);        	

            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rKey).withColumnRange(range).execute();
            Iterator<?> it = opResult.getResult().iterator();
            while (it.hasNext())
            {
                Column<?> col = (Column<?>) it.next();
                                
                bytes += col.getRawName().capacity();
                bytes += col.getByteBufferValue().capacity();
                
                response.append("[")
                		.append(formatResult(col.getRawName(), columnSerializer, compositeColumnSerializers))
                		.append("]:[")
                		.append(formatResult(col.getByteBufferValue(), valueSerializer, compositeValueSerializers))
                		.append("]")
                		.append(SystemUtils.NEW_LINE);
            }
            
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
        return new AstyanaxResponseData(response.toString(), bytes, opResult, rKey, Pair.create(startColumn, endColumn), null);
    }

    
    @Override
	public ResponseData indexRangeSlice(Object indexName,
			Object indexValue, Object startColumn, Object endColumn,
			boolean reversed, int count) throws OperationException {
        int bytes = 0;
        OperationResult<Rows<Object, Object>> opResult = null;
        StringBuffer response = new StringBuffer().append("\n");
        try
        {
        	
        	ByteBufferRange range = buildRange(startColumn, endColumn, reversed, count);        	

            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).searchWithIndex().addExpression().whereColumn(indexName).equals().value(indexValue.toString()).withColumnRange(range).execute();
            
            Iterator<?> rowsIter = opResult.getResult().iterator();
            while(rowsIter.hasNext()){
            	
            	Row<?, ?> row = (Row<?, ?>)rowsIter.next();
            	
            	Log.info("Row key: " + row.getRawKey());
            	response.append("Row: [")
            	.append(formatResult(row.getRawKey(), keySerializer, compositeKeySerializers))
            	.append("]")
            	.append(SystemUtils.NEW_LINE);   
            	
            	Iterator<?> columnIter = row.getColumns().iterator();
            	 while (columnIter.hasNext())
                 {
                     Column<?> col = (Column<?>) columnIter.next();
                                     
                     bytes += col.getRawName().capacity();
                     bytes += col.getByteBufferValue().capacity();
                     
                     response.append("\t[")
                     		.append(formatResult(col.getRawName(), columnSerializer, compositeColumnSerializers))
                     		.append("]:[")
                     		.append(formatResult(col.getByteBufferValue(), valueSerializer, compositeValueSerializers))
                     		.append("]")
                     		.append(SystemUtils.NEW_LINE);
                 }
            }
                                    
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
                
        Map<Object, Object> queryParams = new HashMap<Object, Object>();
        queryParams.put(indexName, indexValue);
        queryParams.put(Pair.create(startColumn, endColumn), null);
        return new AstyanaxResponseData(response.toString(), bytes, opResult, "", queryParams);
	}

	@Override
    public ResponseData delete(Object rkey, Object colName) throws OperationException
    {
        try
        {
            OperationResult<Void> opResult = AstyanaxConnection.instance.keyspace().prepareColumnMutation(cfs, rkey, colName).deleteColumn().execute();
            return new AstyanaxResponseData("", 0, opResult, rkey, colName, null);
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
    }
    
    protected StringBuffer formatResult(ByteBuffer byteBuf, AbstractSerializer<?> serializer, AbstractSerializer<?>[] compositeSerializers){
    	StringBuffer response = new StringBuffer();
    	 
    	if(serializer instanceof CompositeSerializer && compositeSerializers != null && compositeSerializers.length > 0){
         	Composite composite = new Composite();
         	composite.setSerializersByPosition(compositeSerializers);
         	composite.deserialize(byteBuf);
         	for(Component<?> component : composite.getComponents()){
         		if(component.getValue() != null){
         			response.append(component.getValue().toString());
         		}
         		response.append(":");
         	}
         	
         	if(response.length()>0){
         		response.setLength(response.length()-1);
         	}
         }else{
             response.append(SystemUtils.convertToString(serializer, byteBuf));            	
         }
    	
    	return response;
    }
    
    
    protected ByteBufferRange buildRange(Object startColumn, Object endColumn, boolean reversed, int count){
    	ByteBufferRange range = null;
    	
    	if(columnSerializer instanceof CompositeSerializer && compositeColumnSerializers != null && compositeColumnSerializers.length > 0){
    		range = buildCompositeRange((Composite)startColumn, (Composite)endColumn, reversed, count);
    	}else{
    		range = new RangeBuilder().setStart(startColumn, columnSerializer).setEnd(endColumn, columnSerializer).setLimit(count).setReversed(reversed).build();
    	}
    	
    	return range;
    }
	protected ByteBufferRange buildCompositeRange(Composite startColumn, Composite endColumn, boolean reversed, int count){
		List<Component<?>> startComponents = startColumn.getComponents();
		
		CompositeRangeBuilder rangeBuilder = buildRange(compositeColumnSerializers);
		
		for(int i = 0; i < startComponents.size(); i++){
			Object value = startComponents.get(i).getValue();
			if(i < (startComponents.size()-1)){
				rangeBuilder.withPrefix(value);
			}else{
				Object endValue = endColumn.getComponents().get(i).getValue();
				rangeBuilder.greaterThanEquals(value).lessThanEquals(endValue);
			}
			
		}
		
		if(reversed){
			rangeBuilder.reverse();
		}
		
		return rangeBuilder.limit(count).build();
	}
	
	public static CompositeRangeBuilder buildRange(final AbstractSerializer[] serializers) {
    	
        return new CompositeRangeBuilder() {        	
            
            private int position = 0;
            
            public void nextComponent() {
                position++;
            }

            public void append(ByteBufferOutputStream out, Object value, Equality equality) {
                AbstractSerializer serializer = serializers[position];
                // First, serialize the ByteBuffer for this component
                ByteBuffer cb;
                try {
                    cb = serializer.toByteBuffer(value);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (cb == null) {
                    cb = EMPTY_BYTE_BUFFER;
                }

                // Write the data: <length><data><0>
                out.writeShort((short) cb.remaining());
                out.write(cb.slice());
                out.write(equality.toByte());
            }
        };
    }
}
