package com.atguigu.dw.gmall.mock.util

import java.{lang, util}

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index, Search, SearchResult}

object ESUtil {
    private val esUrl = "http://localhost:9200"
    private val factory = new JestClientFactory
    private val conf: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
            .multiThreaded(true)
            .maxTotalConnection(20)
            .connTimeout(10000)
            .readTimeout(10000)
            .build()
    factory.setHttpClientConfig(conf)

    // 获取客户端
    def getESClient = factory.getObject

    // 插入单条数据
    def insertSingle(indexName: String, source: Any) = {
        val client: JestClient = getESClient
        val index: Index = new Index.Builder(source)
                .`type`("_doc")
                .index(indexName)
                .build()
        client.execute(index)
        client.close()
    }

    // 插入多条数据 sources:   Iterable[(id, caseClass)] 或者 Iterable[caseClass]
    def insertBulk(indexName: String, sources: Iterator[Any]): Unit = {
        if (sources.isEmpty) return

        val client: JestClient = getESClient
        val bulkBuilder = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType("_doc")
        sources.foreach { // 把所有的source变成action添加buck中
            //传入的是值是元组, 第一个表示id
            case (id: String, data) => bulkBuilder.addAction(new Index.Builder(data).id(id).build())
            // 其他类型 没有id, 将来省的数据会自动生成默认id
            case data => bulkBuilder.addAction(new Index.Builder(data).build())
        }
        client.execute(bulkBuilder.build())
        closeClient(client)
    }

    /**
      * 关闭客户端
      *
      * @param client
      */
    def closeClient(client: JestClient) = {
        if (client != null) {
            try {
                client.shutdownClient()
            } catch {
                case e => e.printStackTrace()
            }
        }
    }
    def getSaleDetailAndAggResultByAggField(date: String, keyword: String, startPage: Int, size: Int, aggField: String, aggSize: Int): Map[String, Any] = {

        // 统计每个年龄购买情况
        val searchDSL =
            s"""
               |{
               |  "from": ${(startPage - 1) * size},
               |  "size": $size,
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "dt": "$date"
               |        }
               |      }
               |      , "must": [
               |        {"match": {
               |          "sku_name": {
               |            "query": "$keyword",
               |            "operator": "and"
               |          }
               |        }}
               |      ]
               |    }
               |  }
               |  , "aggs": {
               |    "groupby_$aggField": {
               |      "terms": {
               |        "field": "user_$aggField",
               |        "size": $aggSize
               |      }
               |    }
               |  }
               |}
         """.stripMargin

        val search: Search = new Search.Builder(searchDSL)
                .addIndex("gmall_sale_detail")
                .addType("_doc")
                .build()

        val client: JestClient = ESUtil.getESClient
        val result: SearchResult = client.execute(search)

        // 1. 得到总数
        val total: lang.Long = result.getTotal
        // 2. 得到明细 (scala 集合)
        val detailList = List[Map[String, Any]]() // 存储明细
        val hits: util.List[SearchResult#Hit[util.HashMap[String, Any], Void]] = result.getHits(classOf[util.HashMap[String, Any]])
        import scala.collection.JavaConversions._ // 要是使用 scala 的遍历凡是, 需要隐式转换
        for (hit <- hits) {
            val source: util.HashMap[String, Any] = hit.source
            detailList.add(source.toMap)
        }
        // 3. 得到聚合结果
        var aggMap = Map[String, Long]() // 存储聚合结果
        val buckets = result.getAggregations.getTermsAggregation(s"groupby_$aggField").getBuckets
        for (bucket <- buckets) {
            aggMap += bucket.getKey -> bucket.getCount()
        }

        // 返回最终结果
        Map("total" -> total, "aggMap" -> aggMap, "detail" -> detailList)
    }


}
