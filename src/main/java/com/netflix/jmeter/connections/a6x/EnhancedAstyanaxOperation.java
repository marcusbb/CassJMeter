package com.netflix.jmeter.connections.a6x;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.utils.Pair;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.AbstractComposite.Component;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.jmeter.connections.a6x.AstyanaxOperation.AstyanaxResponseData;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.utils.SystemUtils;

/**
 * Extended this class to get more control on the response for Composite Types in the Value.
 * Put in this package because the current AstyanaxOperation class has a package friendly constructor only.
 * @author dvitorino
 *
 */
public class EnhancedAstyanaxOperation extends AstyanaxOperation{
	private static byte END_OF_COMPONENT = 0;
    private static ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
	private static final CompositeSerializer cs = CompositeSerializer.get();
	
	private AbstractSerializer keySerializer;
	private AbstractSerializer columnSerializer;
    private AbstractSerializer valueSerializer;
    
    private AbstractSerializer<?>[] compositeKeySerializers;
    private AbstractSerializer<?>[] compositeColumnSerializers;
    private AbstractSerializer<?>[] compositeValueSerializers;
    
    private ColumnFamily<Object, Object> cfs; 

	private final String cfName;

    EnhancedAstyanaxOperation(String columnName, boolean isCounter) {
		super(columnName, isCounter);	
		 this.cfName = columnName;
	}

    protected StringBuffer formatResult(ByteBuffer byteBuf, AbstractSerializer<?> serializer, AbstractSerializer<?>[] compositeSerializers){
    	StringBuffer response = new StringBuffer();
    	 
    	if(serializer instanceof CompositeSerializer){
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
	
	@Override
    public ResponseData get(Object rkey, Object colName) throws OperationException
    {		
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        OperationResult<Column<Object>> opResult = null;
        try
        {
            opResult = EnhancedAstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rkey).getColumn(colName).execute();
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
    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        int bytes = 0;
        OperationResult<ColumnList<Object>> opResult = null;
        StringBuffer response = new StringBuffer().append("\n");
        try
        {
        	ByteBufferRange range = null;
        	
        	if(columnSerializer instanceof CompositeSerializer){
        		range = buildCompositeRange((Composite)startColumn, (Composite)endColumn, reversed, count);
        	}else{
        		range = new RangeBuilder().setStart(startColumn, columnSerializer).setEnd(endColumn, columnSerializer).setLimit(count).setReversed(reversed).build();
        	}

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
    public void serlizers(AbstractSerializer<?> keySerializer, AbstractSerializer<?> columnSerializer, AbstractSerializer<?> valueSerializer)
    {
    	super.serlizers(keySerializer, columnSerializer, valueSerializer);
    	
        this.cfs = new ColumnFamily(cfName, keySerializer, columnSerializer);
        this.keySerializer = keySerializer;
        this.columnSerializer = columnSerializer;
        this.valueSerializer = valueSerializer;
    }
    
    public void compositeSerializers(AbstractSerializer<?>[] compositeKeySerializers, AbstractSerializer<?>[] compositeColumnSerializers, AbstractSerializer<?>[] compositeValueSerializers){
    	this.compositeKeySerializers = compositeKeySerializers;
    	this.compositeColumnSerializers = compositeColumnSerializers;
    	this.compositeValueSerializers = compositeValueSerializers;
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
