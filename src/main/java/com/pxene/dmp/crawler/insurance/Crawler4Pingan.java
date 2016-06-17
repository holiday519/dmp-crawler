package com.pxene.dmp.crawler.insurance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.IPageCrawler;


public class Crawler4Pingan implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(Crawler4Pingan.class.getName());
    
    private static final String HBASE_TABLE_NAME = "t_insurance_baseinfo";
    
    private static final String HBASE_COLUMN_FAMILY = "insurance_info";
    
    private static final String HBASE_ROWKEY_PREFIX = "00160112_";
    
    private static final JsonArray inputArray = new JsonArray();
    static
    {
        inputArray.add(buildInsuranceJSONObj("MPAY006", "意外保险", "交通意外险", "18到80周岁", "205万", "408万", "1320万"));
        inputArray.add(buildInsuranceJSONObj("MPAY118A", "意外保险", "全车无忧（成人）", "18到70周岁", "202万", "215万", "240万"));
        inputArray.add(buildInsuranceJSONObj("MPAY118B", "意外保险", "全车无忧（未成年）", "0到18周岁", "121万", "122万", "null"));
        inputArray.add(buildInsuranceJSONObj("M53056", "意外保险", "航空意外险", "0到80周岁", "20万", "500万", "500万"));
        inputArray.add(buildInsuranceJSONObj("M00107", "意外保险", "驾驶人意外伤害险", "18到65周岁", "12万", "24万", "60万"));
        inputArray.add(buildInsuranceJSONObj("MPAY112", "意外保险", "短期意外险", "18到64周岁", "12万", "22万", "22万"));
        inputArray.add(buildInsuranceJSONObj("MPAY110", "意外保险", "一年期综合意外险", "18到65周岁", "142万", "165万", "190万"));
        inputArray.add(buildInsuranceJSONObj("MPAY008", "旅游出行", "国内自驾游保险", "18到80周岁", "11万", "null", "null"));
        inputArray.add(buildInsuranceJSONObj("MPAY007", "旅游出行", "国内自助游保险", "18到80周岁", "50万", "165万", "190.1万"));
        inputArray.add(buildInsuranceJSONObj("M53015", "旅游出行", "航空延误", "0到80周岁", "200元/1次", "200元/2次", "200元/3次"));
        inputArray.add(buildInsuranceJSONObj("MNA12", "财产安全", "家庭财产保险", "无限制", "61万", "125万", "270万"));
    }
    
    
    
    @Override
    public void doCrawl() throws IOException
    {
        // 声明需要保存至HBase的数据的结果集：
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        JsonObject tmpObj = null;
        String code = "";
        String type = "";
        String name = "";
        String crowd = "";
        String basicCompensate = "";
        
        for (int i = 0; i < inputArray.size(); i++)
        {
            tmpObj = inputArray.get(i).getAsJsonObject();
            code = tmpObj.get("code").getAsString(); 
            type = tmpObj.get("type").getAsString(); 
            name = tmpObj.get("name").getAsString(); 
            crowd = tmpObj.get("crowd").getAsString(); 
            basicCompensate = tmpObj.get("basicCompensate").getAsString(); 
            
            String rowKey = HBASE_ROWKEY_PREFIX + code;
            
            insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "insurance_type", Bytes.toBytes(type));
            insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "insurance_name", Bytes.toBytes(name));
            insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "insurance_crowd", Bytes.toBytes(crowd));
            insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "insurance_pay", Bytes.toBytes(basicCompensate));
        }
        
        if (preparedData.size() > 0)
        {
            Table table = HBaseTools.openTable(HBASE_TABLE_NAME);
            if (table != null)
            {
                HBaseTools.putRowDatas(table, preparedData);
                HBaseTools.closeTable(table);
            }
        }
        
        for (Entry<String, Map<String, Map<String, byte[]>>> entry : preparedData.entrySet())
        {
            logger.info(entry.getKey() + " == " + entry.getValue());
        }
        logger.info(preparedData.size());
    }
    
    
    protected Map<String, Map<String, Map<String, byte[]>>> insertData(Map<String, Map<String, Map<String, byte[]>>> rowDatas, String rowKey, String familyName, String columnName, byte[] columnVal)
    {
        if (rowDatas.containsKey(rowKey))
        {
            rowDatas.put(rowKey, insertData(rowDatas.get(rowKey), familyName, columnName, columnVal));
        }
        else
        {
            rowDatas.put(rowKey, insertData(new HashMap<String, Map<String, byte[]>>(), familyName, columnName, columnVal));
        }
        return rowDatas;
    }
    
    protected Map<String, Map<String, byte[]>> insertData(Map<String, Map<String, byte[]>> familyDatas, String familyName, String columnName, byte[] columnVal)
    {
        if (familyDatas.containsKey(familyName))
        {
            familyDatas.put(familyName, insertData(familyDatas.get(familyName), columnName, columnVal));
        }
        else
        {
            familyDatas.put(familyName, insertData(new HashMap<String, byte[]>(), columnName, columnVal));
        }
        return familyDatas;
    }
    
    protected Map<String, byte[]> insertData(Map<String, byte[]> columnDatas, String columnName, byte[] columnVal)
    {
        columnDatas.put(columnName, columnVal);
        return columnDatas;
    }
    

    private static JsonObject buildInsuranceJSONObj(String code, String type, String name, String crowd, String basicCompensate, String standerCompensate, String intensiveCompensate)
    {
        JsonObject object = null;
        object = new JsonObject();
        object.addProperty("code", code);
        object.addProperty("type", type);
        object.addProperty("name", name);
        object.addProperty("crowd", crowd);
        object.addProperty("basicCompensate", basicCompensate);
        object.addProperty("standerCompensate", standerCompensate);
        object.addProperty("intensiveCompensate", intensiveCompensate);
        return object;
    }
    
    public static void main(String[] args) throws IOException
    {
        Crawler4Pingan crawler = new Crawler4Pingan();
        crawler.doCrawl();
    }
}