package com.rim.icrs.cassjmeterext.sampler;

import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;

public class EnhancedBatchPutSampler extends EnhancedAbstractSampler
{
    private static final long serialVersionUID = 6393722552275749483L;
    public static final String NAME_AND_VALUE = "NAME_AND_VALUE";
    public static final String NAME_VALUE_SEPARATOR = "NAME_VALUE_SEPARATOR";
    
    public static final String IS_Batch = "IS_Batch";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), isCounter());
        setSerializers(ops);
        Map<?, ?> nv = getNameValue();
        return ops.batchMutate(getKey(), nv);
    }

    public Map<?, ?> getNameValue()
    {    	    	
        Map<Object, Object> return_ = Maps.newHashMap();
        String text = getProperty(NAME_AND_VALUE).getStringValue();
        for (String str : text.split("[\\r\\n]+"))
        {
            String[] cv = str.split(Pattern.quote(getNameValueSeparator()), 2);
            Object colName = convert(cv[0], getCSerializerType(), getCompositeCSerializerTypes());
            Object colValue = convert(cv[1], getVSerializerType(), getCompositeVSerializerTypes());
            return_.put(colName, colValue);
        }
        return return_;
    }

    public void setNameValue(String text)
    {
        setProperty(NAME_AND_VALUE, text);
    }
    
    public String getNameValueSeparator(){
    	return getProperty(NAME_VALUE_SEPARATOR).getStringValue();
    }
    
    public void setNameValueSeparator(String nameValueSeparator){
    	setProperty(NAME_VALUE_SEPARATOR, nameValueSeparator);
    }
}
