package com.pxene.dmp.crawler.tianqi2345;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.common.StringUtils;

public class Application implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(Application.class.getName());
    
    private static final String HBASE_TABLE_NAME = "pxene_mc_weather";
    
    private static final String HBASE_ROWKEY_PREFIX = "00050011";
    
    private static final String HBASE_COLUMN_FAMILY = "info";

    
    
    @Override
    public void doCrawl(String[] args) throws Exception
    {
        String filePath = args[0];
        System.out.println("<= TONY => FilePath: " + filePath);
        Map<String, AreaPojo> areaMap = initArea(filePath);
        rebuildArea(areaMap);
        insertIntoHBase(areaMap);
    }

    
    public static void main(String[] args) throws IOException
    {
        Map<String, AreaPojo> areaMap = initArea("D:\\weather2345.txt");
        for (Map.Entry<String, AreaPojo> entry : areaMap.entrySet())
        {
            System.out.println(entry.getKey() + " * * * " + entry.getValue());
        }
        
        rebuildArea(areaMap);
        
        for (Map.Entry<String, AreaPojo> entry : areaMap.entrySet())
        {
            System.out.println(entry.getKey() + " @ @ @ " + entry.getValue());
        }
        System.out.println(areaMap.size());
    }
    
    
    private static void insertIntoHBase(Map<String, AreaPojo> areaMap) throws IOException
    {
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        for (Map.Entry<String, AreaPojo> entry : areaMap.entrySet())
        {
            String rowKey = HBASE_ROWKEY_PREFIX + "_" + entry.getKey();
            AreaPojo areaPojo = entry.getValue();
            
            String code = areaPojo.getCode();
            String name = areaPojo.getName();
            String parentCode = areaPojo.getBelongToCode();
            String parentName = areaPojo.getBelongToName();
            String full_name = name;
            if (!name.equals(parentName))
            {
                full_name = parentName + "," + full_name; 
            }
            
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "code", Bytes.toBytes(code));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "name", Bytes.toBytes(name));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "parent_code", Bytes.toBytes(parentCode));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "parent_name", Bytes.toBytes(parentName));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "full_name", Bytes.toBytes(full_name));
        }
        
        HBaseTools.insertIntoHBase(HBASE_TABLE_NAME, preparedData);
    }
    
    
    private static Map<String, AreaPojo> initArea(String fileName) throws IOException
    {
        List<String> lines = getSourceLineList(fileName);
        
        Map<String, AreaPojo> areaMap = new HashMap<String, AreaPojo>();
        for (String line : lines)
        {
            if (line.contains("|"))
            {
                String[] tmpArr = line.split("\\|");
                for (String tmpStr : tmpArr)
                {
                    Pattern pattern = Pattern.compile("(\\w+)\\-[A-Z]\\s(.*)\\-(\\d+)");
                    Matcher matcher = pattern.matcher(tmpStr);
                    if (matcher.find()) 
                    {
                        logger.debug(" -----> " + matcher.group(1) + "@" + matcher.group(2) + "@" + matcher.group(3));
                        String code = matcher.group(1);
                        String name = matcher.group(2);
                        String belongToCode = matcher.group(3);
                        
                        if (code.startsWith("a"))
                        {
                            code = code.substring(1, code.length());
                        }
                        
                        AreaPojo areaPojo = new AreaPojo(code, name, belongToCode, null);
                        areaMap.put(code, areaPojo);
                    }
                }
            }
        }
        
        return areaMap;
    }

    private static void rebuildArea(Map<String, AreaPojo> areaMap) throws IOException
    {
        for (Map.Entry<String, AreaPojo> entry : areaMap.entrySet())
        {
            AreaPojo area = entry.getValue();
            String currentCode = area.getCode();
            String belongToCode = area.getBelongToCode();
            if (currentCode.equals(belongToCode))
            {
                area.setBelongToName(area.getName());
            }
            else
            {
                AreaPojo belongToObj = (AreaPojo) areaMap.get(belongToCode);
                String belongToName = belongToObj.getName();
                area.setBelongToName(belongToName);
            }
        }
    }

    private static List<String> getSourceLineList(String fileName) throws IOException
    {
        List<String> result = new ArrayList<String>();
        
        List<String> readLines = FileUtils.readLines(new File(fileName));
        for (String line : readLines)
        {
            String content = "";
            if (line.startsWith("prov["))
            {
                // 'aaa|bbb' --> aaa|bbb
                content = StringUtils.regexpExtract(line, "'(.*)'");
                logger.debug(" ### > " + content);
            }
            if (line.startsWith("provqx["))
            {
                // ['aaa','bbb','ccc'] --> aaa','bbb','ccc
                content = StringUtils.regexpExtract(line, "=\\['(.*)'\\]");
                logger.debug(" *** > " + content);
            }
            
            if (content.contains("','"))
            {
                String[] tmpArr = content.split("','");
                result.addAll(Arrays.asList(tmpArr));
            }
            else
            {
                result.add(content);
            }
        }
        return result;
    }
}

class AreaPojo
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