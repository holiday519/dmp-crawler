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
    private static final String HIVE_URL = "jdbc:hive2://192.168.3.171:10000/basetables";
    
    /**
     * Hive 用户名
     */
    private static final String HIVE_USERNAME = "ningyu";
    
    
    public WXMetaDataGenerator()
    {
    }
    
    
    public List<Product> generate(String dataStr) throws IOException, SQLException
    {
        List<Product> result = new ArrayList<Product>();
        
        Connection con = null;
        try
        {
            Class.forName(driverName);
            
            con = DriverManager.getConnection(HIVE_URL, HIVE_USERNAME, "");
            
            Statement stmt = con.createStatement();
            
            logger.info("正在查询Hive表：‘weixin’，请稍候......");
            
            //ResultSet res = stmt.executeQuery("select biz,mid,idx,sn from weixin where biz != '' and mid != '' and idx != '' and sn != '' and data_time LIKE '20160709%' group by biz, mid, idx, sn");
            String sql = "select biz,mid,idx,sn from weixin where biz != '' and mid != '' and idx != '' and sn != '' and data_time LIKE '" + dataStr +"%' group by biz, mid, idx, sn";
            logger.info("查询SQL：" + sql);
            ResultSet res = stmt.executeQuery(sql);
            
            logger.info("Scan Result:");
            logger.info("biz \t mid \t idx \t sn ");
            
            Product product = null;
            String biz = "";
            String mid = "";
            String idx = "";
            String sn = "";
            
            while (res.next()) 
            {
                biz = res.getString(1);
                mid = res.getString(2);
                idx = res.getString(3);
                sn = res.getString(4);
                product = new Product(biz, mid, idx, sn);
                product.setDateStr(dataStr);
                result.add(product);
            }
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