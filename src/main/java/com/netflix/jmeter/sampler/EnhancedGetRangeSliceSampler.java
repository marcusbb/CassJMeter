package com.rim.icrs.cassjmeterext.sampler;

import com.netflix.jmeter.connections.a6x.EnhancedAstyanaxOperation;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;

public class EnhancedGetRangeSliceSampler extends EnhancedAbstractSampler
{
	private static final long serialVersionUID = -2011040387860046047L;

    public static final String START_COLUMN_NAME = "START_COLUMN_NAME";
    public static final String END_COLUMN_NAME = "END_COLUMN_NAME";
    public static final String IS_REVERSE = "IS_REVERSE";
    public static final String COUNT = "COUNT";

	public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), false);
        setSerializers(ops);
        
        if(ops instanceof EnhancedAstyanaxOperation){
        	setCompositeSerializers((EnhancedAstyanaxOperation)ops);
        }
              
        return ops.rangeSlice(getKey(), getStartName(), getEndName(), isReverse(), getCount());
    }    
		
    public void setStartName(String text)
    {
        setProperty(START_COLUMN_NAME, text);
    }

    public void setEndName(String text)
    {
        setProperty(END_COLUMN_NAME, text);
    }

    public Object getStartName()
    {
        String text = getProperty(START_COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType(), getCompositeCSerializerTypes());
    }

    public Object getEndName()
    {
        String text = getProperty(END_COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType(), getCompositeCSerializerTypes());
    }

    public boolean isReverse()
    {
        return getPropertyAsBoolean(IS_REVERSE);
    }

    public void setReverse(boolean isReverse)
    {
        setProperty(IS_REVERSE, isReverse);
    }

    public void setCount(String text)
    {
        setProperty(COUNT, text);
    }

    public int getCount()
    {
        return getProperty(COUNT).getIntValue();
    }

}
