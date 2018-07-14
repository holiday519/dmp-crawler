package com.pxene.dmp.crawler.gpsspg;

public enum Enum4ENC
{
    CHINA_MOBILE("00"),
    CHINA_UNICOM("01"),
    CHINA_TELECOM_4G("11");
    
    
    private String code;
    
    
    public String getCode()
    {
        return code;
    }
    
    public void setCode(String code)
    {
        this.code = code;
    }
    
    
    Enum4ENC(String code)
    {
        this.code = code;
    }
}
