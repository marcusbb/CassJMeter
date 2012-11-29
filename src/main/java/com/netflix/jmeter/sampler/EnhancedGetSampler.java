package com.rim.icrs.cassjmeterext.sampler;

import com.netflix.jmeter.connections.a6x.EnhancedAstyanaxOperation;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;

public class EnhancedGetSampler extends EnhancedAbstractSampler
{
	private static final long serialVersionUID = -2011040387860046047L;

	public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), false);
        setSerializers(ops);
        
        if(ops instanceof EnhancedAstyanaxOperation){
        	setCompositeSerializers((EnhancedAstyanaxOperation)ops);
        }
              
        return  ops.get(getKey(), getColumnName());
    }    
}
