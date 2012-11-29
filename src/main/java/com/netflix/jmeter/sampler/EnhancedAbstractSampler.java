package com.netflix.jmeter.sampler;

import java.util.Set;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;

public abstract class EnhancedAbstractSampler extends AbstractSampler{
	
	private static final long serialVersionUID = 2763879697867714542L;
	
	public static final String COMPOSITE_KEY_SERIALIZER_TYPES = "C_KEY_SERIALIZER_TYPES";
	public static final String COMPOSITE_COLUMN_SERIALIZER_TYPES = "C_COLUMN_SERIALIZER_TYPES";
	public static final String COMPOSITE_VALUE_SERIALIZER_TYPES = "C_VALUE_SERIALIZER_TYPES";

	static
	{
		AbstractSampler.serializers.put("CompositeSerializer", CompositeSerializer.get());
	}

	public String[] getCompositeKSerializerTypes()
    {
        return getAsArray(getProperty(COMPOSITE_KEY_SERIALIZER_TYPES).getStringValue(), ",");
    }
	
    public String[] getCompositeCSerializerTypes()
    {
        return getAsArray(getProperty(COMPOSITE_COLUMN_SERIALIZER_TYPES).getStringValue(), ",");
    }
   
    public String[] getCompositeVSerializerTypes()
    {
        return getAsArray(getProperty(COMPOSITE_VALUE_SERIALIZER_TYPES).getStringValue(), ",");
    }

    public void setCompositeKSerializerTypes(String[] serializerTypes)
    {
        setProperty(COMPOSITE_KEY_SERIALIZER_TYPES, getAsString(serializerTypes, ","));
    }

    public void setCompositeCSerializerTypes(String[] serializerTypes)
    {
        setProperty(COMPOSITE_COLUMN_SERIALIZER_TYPES, getAsString(serializerTypes, ","));
    }
    
    public void setCompositeVSerializerTypes(String[] serializerTypes)
    {
        setProperty(COMPOSITE_VALUE_SERIALIZER_TYPES, getAsString(serializerTypes, ","));
    }
    
    @Override
    public void setKey(String text)
    {
        setProperty(KEY, text);
    }

    @Override
    public Object getKey()
    {
        String text = getProperty(KEY).getStringValue();
        return convert(text, getKSerializerType(), getCompositeKSerializerTypes());
    }
    
    public Object getColumnName()
    {
        String text = getProperty(COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType(), getCompositeCSerializerTypes());
    }
    
    public static String[] getAsArray(String delimitedList, String delimiter){
    	if(delimitedList != null){
    		return delimitedList.split(delimiter);
    	}
    	return null;
    }
    
    public static String getAsString(String[] array, String delimiter){
    	StringBuffer buf = new StringBuffer("");
    	if(array != null && array.length > 0){
    		for(String str : array){
    			buf.append(str).append(delimiter);
    		
    		}
    		
    		buf.setLength(buf.length()-delimiter.length());
    	}
    	
    	return buf.toString();
    }
    
    public Object convert(String text, String serializerType, String[] compositeSerializerTypes){
    	
    	if(serializerType.equals("CompositeSerializer")){
    		Composite composite = new Composite();    		
    		if(text != null){
    			String[] components = text.split(":");
        		for(int i = 0; i < components.length; i++){
        			String partSerializerType = (i < compositeSerializerTypes.length)
        											?compositeSerializerTypes[i]:"StringSerializer";

        			Object conversionResult = super.convert(components[i], partSerializerType);
        			if(conversionResult != null){
        				composite.add(super.convert(components[i], partSerializerType));
        			}        		        			
        		}    			
    		}else{
    			composite.add(new byte[0]);
    		}
    		
    		return composite;
    	}else{
    		return super.convert(text, serializerType);
    	}    	
    }
        
    public static Set<String> getSerializerNames()
    {
    	return AbstractSampler.getSerializerNames();        
    }
    
    public void setCompositeSerializers(Operation ops)
    {       
        ops.compositeSerializers(serializers(getCompositeKSerializerTypes()), serializers(getCompositeCSerializerTypes()), serializers(getCompositeVSerializerTypes()));
    }

    public static AbstractSerializer<?>[] serializers(String[] serializerNames)
    {
    	if(serializerNames == null){
    		return null;
    	}
    	
    	AbstractSerializer<?>[] compositeSerializers = new AbstractSerializer[serializerNames.length];
    	for(int i = 0; i < serializerNames.length; i++){
    		compositeSerializers[i] = serializers.get(serializerNames[i]);
    	}
    	
        return compositeSerializers;
    }
}
