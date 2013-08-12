package org.dcache.cdmi;

import diskCacheV111.vehicles.Message;

public class CDMIMessage extends Message
{
    private final String name;
    private String greeting;

    public CDMIMessage(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public String getGreeting()
    {
        return greeting;
    }

    public void setGreeting(String greeting)
    {
        this.greeting = greeting;
    }
}

