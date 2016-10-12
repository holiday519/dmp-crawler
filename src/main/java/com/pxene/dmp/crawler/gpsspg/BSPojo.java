package com.pxene.dmp.crawler.gpsspg;

public class BSPojo
{
    private String id;
    private String lat;
    private String lng;
    private String radius;
    private String address;
    private String roads;
    private String rid;
    private String rids;
    
    
    public String getId()
    {
        return id;
    }
    public void setId(String id)
    {
        this.id = id;
    }
    public String getLat()
    {
        return lat;
    }
    public void setLat(String lat)
    {
        this.lat = lat;
    }
    public String getLng()
    {
        return lng;
    }
    public void setLng(String lng)
    {
        this.lng = lng;
    }
    public String getRadius()
    {
        return radius;
    }
    public void setRadius(String radius)
    {
        this.radius = radius;
    }
    public String getAddress()
    {
        return address;
    }
    public void setAddress(String address)
    {
        this.address = address;
    }
    public String getRoads()
    {
        return roads;
    }
    public void setRoads(String roads)
    {
        this.roads = roads;
    }
    public String getRid()
    {
        return rid;
    }
    public void setRid(String rid)
    {
        this.rid = rid;
    }
    public String getRids()
    {
        return rids;
    }
    public void setRids(String rids)
    {
        this.rids = rids;
    }
    
    
    public BSPojo()
    {
        super();
    }
    public BSPojo(String id, String lat, String lng, String radius, String address, String roads, String rid, String rids)
    {
        super();
        this.id = id;
        this.lat = lat;
        this.lng = lng;
        this.radius = radius;
        this.address = address;
        this.roads = roads;
        this.rid = rid;
        this.rids = rids;
    }
    
    
    @Override
    public String toString()
    {
        return "BSPojo [id=" + id + ", lat=" + lat + ", lng=" + lng + ", radius=" + radius + ", address=" + address + ", roads=" + roads + ", rid=" + rid + ", rids=" + rids + "]";
    }
}
