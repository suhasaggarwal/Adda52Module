package com.spidio.UserSegmenter;

import java.io.PrintStream;
import java.util.Map;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

public class Indexcloner
{
  public static void main(String[] args)
    throws Exception
  {
    Client client = new TransportClient()
      .addTransportAddress(new InetSocketTransportAddress(
      "172.16.101.132", 9300));
    
    String keywords = null;
    String description = null;
    String[] searchkeyword = null;
    String[] finalsearchkeyword = null;
    SearchHit[] matchingsegmentrecords = null;
    String channel_name = null;
    String subcategory = null;
    String referrer = null;
    String postdate = null;
    String cookie_id = null;
    SearchHit[] results = IndexCategoriesData.searchEntireUserData(client, "enhanceduserdatabeta1", "core2");
    SearchHit[] arrayOfSearchHit1;
    int j = (arrayOfSearchHit1 = results).length;
    for (SearchHit hit:results )
    {
     
      
      System.out.println("------------------------------");
      Map<String, Object> result = hit.getSource();
      referrer = (String)result.get("referrer");
      System.out.println(referrer);
      channel_name = (String)result.get("channel_name");
      postdate = (String)result.get("request_time");
      cookie_id = (String)result.get("cookie_id");
      String documentId = hit.getId();
      
      
      if ((channel_name.equals("Adda52")))
      {
        result.put("channel_name","Adda52v1");
        
    	IndexCategoriesData.postElasticSearch(client,result);
        System.out.println(documentId + ":" + referrer);
      }
    
      
    }
  }
}
