package com.pxene.dmp.crawler.tianqi;

public class AreaPojo
{
    private String code;
    private String name;
    private String belongToCode;
    private String belongToName;
    
    
    public String getCode()
    {
        return code;
    }
    public void setCode(String code)
    {
        this.code = code;
    }
    public String getName()
    {
        return name;
    }
    public void setName(String name)
    {
        this.name = name;
    }
    public String getBelongToCode()
    {
        return belongToCode;
    }
    public void setBelongToCode(String belongToCode)
    {
        this.belongToCode = belongToCode;
    }
    public String getBelongToName()
    {
        return belongToName;
    }
    public void setBelongToName(String belongToName)
    {
        this.belongToName = belongToName;
    }
    
    
    public AreaPojo()
    {
        super();
    }
    public AreaPojo(String code, String name, String belongToCode, String belongToName)
    {
        super();
        this.code = code;
        this.name = name;
        this.belongToCode = belongToCode;
        this.belongToName = belongToName;
    }
    
    
    @Override
    public String toString()
    {
        return "AreaPojo [code=" + code + ", name=" + name + ", belongToCode=" + belongToCode + ", belongToName=" + belongToName + "]";
    }
}
