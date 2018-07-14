package com.pxene.dmp.crawler.social.worker;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;


public class WXSolrIndexBuilder
{
    private static final String SOLR_CORE_URL = "http://dmp06:8983/solr/weixin_core_prod";
    private static HttpSolrClient client = null;
    
    
    public WXSolrIndexBuilder()
    {
        client = new HttpSolrClient(SOLR_CORE_URL);
    }
    
    
    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        client.close();
    }
    
    
    /**
     * 在Solr中建立索引.
     * @param officialAccount
     * @throws IOException
     * @throws SolrServerException
     */
    public int doBuildIndex(OfficialAccount officialAccount) throws IOException, SolrServerException
    {
        int result = 0;
        
        String biz = officialAccount.getMetaData().getBiz();
        
        SolrInputDocument inputDocument = new SolrInputDocument();
        inputDocument.addField("id", biz);
        inputDocument.addField("wx_biz", biz);
        inputDocument.addField("wx_name", officialAccount.getWeixinName());
        
        System.out.println(inputDocument);
        
        UpdateResponse response = client.add(inputDocument);
        
        result = response.getStatus();
        
        client.commit();
        
        return result;
    }
    
    
    /**
     * 从Solr中删队索引.
     * @param query
     * @return
     * @throws SolrServerException
     * @throws IOException
     */
    public static int doDestoryIndex(String query) throws SolrServerException, IOException
    {
        int result = 0;
        
        if (query == null || "".equals(query))
        {
            query = "*:*";
        }
        
        UpdateResponse response = client.deleteByQuery(query);
        
        result = response.getStatus();
        
        client.commit();
        
        return result;
    }
}