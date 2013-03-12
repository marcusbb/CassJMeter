package com.netflix.jmeter.sampler;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import com.netflix.astyanax.index.IndexMetadata;

public class EnhancedGetHCIndexRangeSliceSampler extends EnhancedAbstractSampler
{    
	private static final long serialVersionUID = -3385053564788259243L;
	public static final String INDEX_COLUMN_FAMILY = "INDEX_COLUMN_FAMILY";

    public static final String INDEX_NAME_AND_VALUE = "INDEX_NAME_AND_VALUE";
    public static final String INDEX_NAME_VALUE_SEPARATOR = "INDEX_NAME_VALUE_SEPARATOR";

    public static final String START_COLUMN_NAME = "START_COLUMN_NAME";
    public static final String END_COLUMN_NAME = "END_COLUMN_NAME";
    public static final String IS_REVERSE = "IS_REVERSE";
    public static final String COUNT = "COUNT";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), getIndexedColumnsMetadata(), false);
        setSerializers(ops);
        return ops.indexRangeSlice(getIndexNameValue(), getStartName(), getEndName(), isReverse(), getCount());
    }

    public void setIndexColumnFamily(String text)
    {
    	setProperty(INDEX_COLUMN_FAMILY, text);
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

    public String getIndexColumnFamily()
    {
        return getProperty(INDEX_COLUMN_FAMILY).getStringValue();
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
       
    public List<IndexMetadata<?,?>> getIndexedColumnsMetadata()
    {        
    	List<IndexMetadata<?,?>> return_ = com.google.common.collect.Lists.newArrayList();
    	
    	Iterator<?> iter = getIndexNameValue().keySet().iterator();
    	
    	while(iter.hasNext()){
    		@SuppressWarnings({ "unchecked", "rawtypes" })
    		IndexMetadata indexMetaData = new IndexMetadata(getColumnFamily(), 
    				iter.next(), 
        			serializerToClassMap.get(getKSerializerType()),
        			getIndexColumnFamily());
            return_.add(indexMetaData);
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
