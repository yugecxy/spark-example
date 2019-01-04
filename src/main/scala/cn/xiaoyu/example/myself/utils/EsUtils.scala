package cn.xiaoyu.example.myself.utils

import cn.xiaoyu.example.myself.utils.util.prop
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.indexes.CreateIndexDefinition
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.{HttpClientConfigCallback, RequestConfigCallback}

import scala.concurrent.duration._


object EsUtils {

  def init_es_index(indexName: String, indexDsl: CreateIndexDefinition): Unit = {
    if (!EsUtils.execute(
      indexExists(indexName)).isExists
    ) {
      EsUtils.execute(indexDsl).acknowledged
    }
  }

  def get_es_client(): HttpClient = {
    lazy val provider = {
      val provider = new BasicCredentialsProvider
      val credentials = new UsernamePasswordCredentials(
        prop.getString("es.net.http.auth.user", ""),
        prop.getString("es.net.http.auth.pass", ""))
      provider.setCredentials(AuthScope.ANY, credentials)
      provider
    }

    HttpClient(ElasticsearchClientUri(prop.getString("es.nodes"), 9200), new RequestConfigCallback {
      override def customizeRequestConfig(requestConfigBuilder: Builder): Builder = {
        requestConfigBuilder
      }
    }, new HttpClientConfigCallback {
      override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
        httpClientBuilder.setDefaultCredentialsProvider(provider)
      }
    })
  }

  def execute[T, U](request: T)(implicit exec: com.sksamuel.elastic4s.http.HttpExecutable[T, U]): U = {
    val client = get_es_client()

    try {
      val duration = Duration(100, SECONDS)
      val result = client.execute(request).await(duration)
      client.close()
      result
    } finally {
      client.close()
    }
  }
}
