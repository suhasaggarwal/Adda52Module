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

public class DataCleaner
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
    
    SearchHit[] results = IndexCategoriesData.searchEntireUserData(client, "enhanceduserdatabeta1", "core2");
    SearchHit[] arrayOfSearchHit1;
    int j = (arrayOfSearchHit1 = results).length;
    for (SearchHit hit:results)
    {
      
      
      System.out.println("------------------------------");
      Map<String, Object> result = hit.getSource();
      referrer = (String)result.get("referrer");
      System.out.println(referrer);
      channel_name = (String)result.get("channel_name");
      postdate = (String)result.get("request_time");
      
      String documentId = hit.getId();
      
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("momagic")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "momagic");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("opera")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "opera");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("cricbuzz")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "cricbuzz");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("cricbuzz_mob")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "cricbuzz_mob");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("m_cricbuzz")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "cricbuzz_mob");
        System.out.println(documentId + ":" + referrer);
      }
      
      
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("taboola")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "taboola");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("forkmedia")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "forkmedia");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("inuxu_native")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "inuxu_native");
        System.out.println(documentId + ":" + referrer);
      }
    
     
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("ixigo")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "ixigo");
        System.out.println(documentId + ":" + referrer);
      }
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("spidio")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "spidio");
        System.out.println(documentId + ":" + referrer);
      }
      
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("sales_24")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "spidio");
        System.out.println(documentId + ":" + referrer);
      }
      
      
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("shopclues")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "shopclues");
        System.out.println(documentId + ":" + referrer);
      }
    
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("m_shopclues")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "shopclues");
        System.out.println(documentId + ":" + referrer);
      }
      
      
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("espn")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "espn");
        System.out.println(documentId + ":" + referrer);
      }
      
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("espn_mob")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "espn");
        System.out.println(documentId + ":" + referrer);
      }
     
      if ((channel_name.equals("Adda52v1")) && (referrer != null) && (!referrer.isEmpty()) && (referrer.contains("gamooga")))
      {
        IndexCategoriesData.updateDocument(client, "enhanceduserdatabeta1", "core2", documentId, "channel_name", "gamooga");
        System.out.println(documentId + ":" + referrer);
      }
      
      
      
    }
  }
}
