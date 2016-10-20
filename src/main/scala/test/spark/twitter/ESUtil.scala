package test.spark.twitter

import java.io.IOException
import java.net.InetAddress
import java.util.concurrent.ExecutionException

import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit

//for elastic search client

object ESUtil {

  private val esclient: Client =  TransportClient.builder().build().addTransportAddress(
    new InetSocketTransportAddress(InetAddress.getByName("10.252.0.246"), 9300))
  //get tweets by id
  def getContentFromES(id: String): String = {
    val queryBuilder = QueryBuilders.prefixQuery("_id", id)
    val searchResponse = esclient.prepareSearch("us_large_cities").setTypes("city")
      .setQuery(queryBuilder)
      .execute()
      .actionGet()
    val hits = searchResponse.getHits
    val searchHists = hits.getHits
    //no found then check null
    if (searchHists.length > 0 && (Option(searchHists(0).getSource.get("content"))!=None)) {
      searchHists(0).getSource.get("content").toString
    } else
    ""
  }
  //update the tweets by location
  def upMethod1(id: String,content:String) {
    try {
      val uRequest = new UpdateRequest()
      uRequest.index("us_large_cities")
      uRequest.`type`("city")
      uRequest.id(id)
      uRequest.doc(jsonBuilder().startObject().field("content", getContentFromES(id) + "@#@" + System.currentTimeMillis + "::" + content)
        .field("updateT", System.currentTimeMillis).endObject())
      esclient.update(uRequest).get
    } catch {
      case e: IOException => e.printStackTrace()
      case e: InterruptedException => e.printStackTrace()
      case e: ExecutionException => e.printStackTrace()
    }
  }
}
