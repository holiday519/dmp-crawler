package com.pxene.dmp.crawler.social.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

public class StringUtils
{
    /**
     * 获得指定日期的前一天的字符串表示，如果不指定日期，则获得当前日期的前一天.
     * 示例：System.out.println(getYestodayStr(null));
     * 示例：System.out.println(getYestodayStr("2016-01-01"));
     * @param theDay yyyy-MM-dd 格式的字符串
     * @return       yyyyMMdd   格式的字符串   
     */
    public static String getYesterdayStr(String theDay)
    {
        Date date = null;
        if (theDay == null || "".equals(theDay))
        {
            date = new Date();
        }
        else
        {
            try
            {
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                date = sdf.parse(theDay);
            }
            catch (ParseException e)
            {
                e.printStackTrace();
                date = new Date();
            }
        }
        
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        
        Date yestodayDate = calendar.getTime();
        return new SimpleDateFormat("yyyyMMdd").format(yestodayDate);
    }
    
    public static String getTheDayBeforeYesterdayStr()
    {
        Date tdby = DateUtils.addDays(new Date(), -2);
        return new SimpleDateFormat("yyyyMMdd").format(tdby);
    }
    
    public static void main(String[] args)
    {
        System.out.println(StringUtils.getYesterdayStr(null)+"%");
        System.out.println(StringUtils.getYesterdayStr("2016-01-01")+"%");
        System.out.println(getTheDayBeforeYesterdayStr()+"%");
    }
}
