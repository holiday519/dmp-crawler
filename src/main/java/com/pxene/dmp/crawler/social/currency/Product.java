package com.pxene.dmp.crawler.social.currency;

public class Product
{
    private int id;
    private String biz;
    private String mid;
    private String idx;
    private String sn;
    private String dateStr;
    
    
    public int getId()
    {
        return id;
    }
    public void setId(int id)
    {
        this.id = id;
    }
    public String getBiz()
    {
        return biz;
    }
    public void setBiz(String biz)
    {
        this.biz = biz;
    }
    public String getMid()
    {
        return mid;
    }
    public void setMid(String mid)
    {
        this.mid = mid;
    }
    public String getIdx()
    {
        return idx;
    }
    public void setIdx(String idx)
    {
        this.idx = idx;
    }
    public String getSn()
    {
        return sn;
    }
    public void setSn(String sn)
    {
        this.sn = sn;
    }
    public Product()
    {
        super();
    }
    public Product(int id)
    {
        super();
        this.id = id;
    }
    public String getDateStr()
    {
        return dateStr;
    }
    public void setDateStr(String dateStr)
    {
        this.dateStr = dateStr;
    }
    
    
    public Product(String biz, String mid, String idx, String sn)
    {
        super();
        this.biz = biz;
        this.mid = mid;
        this.idx = idx;
        this.sn = sn;
    }
    public Product(int id, String biz, String mid, String idx, String sn, String dateStr)
    {
        super();
        this.id = id;
        this.biz = biz;
        this.mid = mid;
        this.idx = idx;
        this.sn = sn;
        this.dateStr = dateStr;
    }
    
    
    @Override
    public String toString()
    {
        return "Product [id=" + id + ", biz=" + biz + ", mid=" + mid + ", idx=" + idx + ", sn=" + sn + ", dateStr=" + dateStr + "]";
    }
}
