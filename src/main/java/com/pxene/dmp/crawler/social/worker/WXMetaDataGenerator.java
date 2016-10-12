package com.pxene.dmp.crawler.social.worker;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.crawler.social.currency.Product;


public class WXMetaDataGenerator
{
    private static Logger logger = LogManager.getLogger(WXMetaDataGenerator.class.getName());
    
    /**
     * Hive JDBC驱动
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    
    /**
     * Hive 连接URL
     */
    private static final String HIVE_URL = "jdbc:hive2://dmp01:10000/basetables";
    
    /**
     * Hive 用户名
     */
    private static final String HIVE_USERNAME = "ningyu";
    
    
    public WXMetaDataGenerator()
    {
    }
    
    
    /**
     * 从Hive中取得biz, mid, idx, sn四个参数，以此构建微信公众号文章的URL，爬取这个URL，解析其内容，保存至HBase中，并创建Solr索引.
     * @param dataStr           需要从Hive中查找的规定日期
     * @param partitionSource   需要从Hive中查找的来源，1：联通；2：电信
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public List<Product> generate(String dataStr, String partitionSource) throws IOException, SQLException
    {
        List<Product> result = new ArrayList<Product>();
        
        Connection con = null;
        try
        {
            Class.forName(driverName);
            
            con = DriverManager.getConnection(HIVE_URL, HIVE_USERNAME, "");
            
            Statement stmt = con.createStatement();
            
            logger.info("正在查询Hive表：‘weixin’，请稍候......");
            
            String sql = "select biz,mid,idx,sn from weixin where biz != '' and mid != '' and idx != '' and sn != '' and data_time LIKE '" + dataStr +"%' and partition_source = '" + partitionSource + "' group by biz, mid, idx, sn";
            logger.info("查询SQL：" + sql);
            ResultSet res = stmt.executeQuery(sql);
            
            logger.info("Scan Result:");
            logger.info("biz \t mid \t idx \t sn ");
            
            Product product = null;
            String biz = "";
            String mid = "";
            String idx = "";
            String sn = "";
            
            int i = 0;
            while (res.next()) 
            {
                biz = res.getString(1);
                mid = res.getString(2);
                idx = res.getString(3);
                sn = res.getString(4);
                product = new Product(biz, mid, idx, sn);
                product.setDateStr(dataStr);
                result.add(product);
                i++;
                System.out.println("<= TONY => Hive result: " + product);
            }
            System.out.println("<= TONY => Total item fetch from Hive: " + i);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return result;
        }
        finally 
        {
            con.close();
        }
        
        return result;
    }
}