package com.netflix.jmeter.sampler;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import com.netflix.astyanax.index.IndexMetadata;

public class EnhancedBatchPutHCIndexSampler extends EnhancedAbstractSampler
{
    private static final long serialVersionUID = 6393722552275749483L;
    public static final String INDEX_COLUMN_FAMILY = "INDEX_COLUMN_FAMILY";
    public static final String NAME_AND_VALUE = "NAME_AND_VALUE";
    public static final String NAME_VALUE_SEPARATOR = "NAME_VALUE_SEPARATOR";
    public static final String INDEX_COLUMN_NAMES = "INDEX_COLUMN_NAMES";
    
    public static final String IS_Batch = "IS_Batch";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), getIndexedColumnsMetadata(), isCounter());
        setSerializers(ops);
        Map<?, ?> nv = getNameValue();
        return ops.batchMutate(getKey(), nv);
    }

    public String getIndexColumnFamily()
    {
        return getProperty(INDEX_COLUMN_FAMILY).getStringValue();
    }
    
    public String getIndexNames()
    {
        return getProperty(INDEX_COLUMN_NAMES).getStringValue();
    }    
    
    public List<IndexMetadata<?,?>> getIndexedColumnsMetadata()
    {
        
    	List<IndexMetadata<?,?>> return_ = com.google.common.collect.Lists.newArrayList();
        String text = getIndexNames();
        for (String indexedColName : text.split(","))
        {            
        	@SuppressWarnings({ "unchecked", "rawtypes" })
			IndexMetadata indexMetaData = new IndexMetadata(getColumnFamily(), 
        			convert(indexedColName, getCSerializerType(), getCompositeCSerializerTypes()), 
        			serializerToClassMap.get(getKSerializerType()),
        			getIndexColumnFamily());
            return_.add(indexMetaData);
        }
        return return_;
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

    public void setIndexColumnFamily(String text)
    {
    	setProperty(INDEX_COLUMN_FAMILY, text);
    }
    
    public void setIndexNames(String text)
    {
    	setProperty(INDEX_COLUMN_NAMES, text);
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
