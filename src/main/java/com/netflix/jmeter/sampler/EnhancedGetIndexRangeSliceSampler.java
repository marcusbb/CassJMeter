package com.netflix.jmeter.sampler;

import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

public class EnhancedGetIndexRangeSliceSampler extends EnhancedAbstractSampler
{
    private static final long serialVersionUID = -8566773644299382213L;
    
    public static final String INDEX_NAME_AND_VALUE = "INDEX_NAME_AND_VALUE";
    public static final String INDEX_NAME_VALUE_SEPARATOR = "INDEX_NAME_VALUE_SEPARATOR";
    
    public static final String START_COLUMN_NAME = "START_COLUMN_NAME";
    public static final String END_COLUMN_NAME = "END_COLUMN_NAME";
    public static final String IS_REVERSE = "IS_REVERSE";
    public static final String COUNT = "COUNT";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), false);
        setSerializers(ops);
        return ops.indexRangeSlice(getIndexNameValue(), getStartName(), getEndName(), isReverse(), getCount());
    }

    public void setIndexNameValue(String nameValueSeparator){
    	setProperty(INDEX_NAME_AND_VALUE, nameValueSeparator);
    }       
    
    public void setIndexNameValueSeparator(String nameValueSeparator){
    	setProperty(INDEX_NAME_VALUE_SEPARATOR, nameValueSeparator);
    } 
    
    public void setStartName(String text)
    {
        setProperty(START_COLUMN_NAME, text);
    }

    public void setEndName(String text)
    {
        setProperty(END_COLUMN_NAME, text);
    }

    public String getIndexNameValueSeparator(){
    	return getProperty(INDEX_NAME_VALUE_SEPARATOR).getStringValue();
    }    
        
    public Map<?, ?> getIndexNameValue()
    {    	    	
        Map<Object, Object> return_ = Maps.newHashMap();
        String text = getProperty(INDEX_NAME_AND_VALUE).getStringValue();
        for (String str : text.split("[\\r\\n]+"))
        {
            String[] cv = str.split(Pattern.quote(getIndexNameValueSeparator()), 2);
            Object colName = convert(cv[0], getCSerializerType(), getCompositeCSerializerTypes());
            Object colValue = convert(cv[1], getVSerializerType(), getCompositeVSerializerTypes());
            return_.put(colName, colValue);
        }
        return return_;
    }
       
    public Object getStartName()
    {
        String text = getProperty(START_COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType());
    }

    public Object getEndName()
    {
        String text = getProperty(END_COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType());
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
