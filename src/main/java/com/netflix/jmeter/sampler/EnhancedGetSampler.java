package com.netflix.jmeter.sampler;

public class EnhancedGetSampler extends EnhancedAbstractSampler
{
	private static final long serialVersionUID = -2011040387860046047L;

	public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), false);
        setSerializers(ops);
        setCompositeSerializers(ops);
              
        return  ops.get(getKey(), getColumnName());
    }    
}
