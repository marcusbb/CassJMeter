package com.netflix.jmeter.connections.a6x;

import com.netflix.jmeter.sampler.Operation;

public class EnhancedAstyanaxConnection extends AstyanaxConnection
{

    public Operation newOperation(String columnName, boolean isCounter)
    {
        return new EnhancedAstyanaxOperation(columnName, isCounter);
    }
}
