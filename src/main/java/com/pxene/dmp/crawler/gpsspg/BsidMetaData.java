package com.pxene.dmp.crawler.gpsspg;

public class BsidMetaData
{
    private String imsi;
    
    private String bsid;
    
    private String datetime;
    
    
    public BsidMetaData()
    {
    }
    public BsidMetaData(String imsi, String bsid, String datetime)
    {
        super();
        this.imsi = imsi;
        this.bsid = bsid;
        this.datetime = datetime;
    }
    
    
    @Override
    public String toString()
    {
        return "BsidMetaData [imsi=" + imsi + ", bsid=" + bsid + ", datetime=" + datetime + "]";
    }
}