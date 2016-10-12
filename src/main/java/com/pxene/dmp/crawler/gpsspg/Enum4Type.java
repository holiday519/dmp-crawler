package com.pxene.dmp.crawler.gpsspg;

public enum Enum4Type
{
    GSM_UMTS_LTE(""),
    CDMA("cdma");
    
    
    private String type;
    
    
    public String getType()
    {
        return type;
    }
    public void setType(String type)
    {
        this.type = type;
    }
    
    
    Enum4Type(String type)
    {
        this.type = type;
    }
}
