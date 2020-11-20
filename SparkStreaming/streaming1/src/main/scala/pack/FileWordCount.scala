package pack

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.spark.{MongoConnector, MongoSpark, toSparkContextFunctions}
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

object FileWordCount {
  val path = "SH600721kline.csv"

  def saveAndUpdate(rdd: RDD[Document], writeConfig: WriteConfig): Unit = {
    val DefaultMaxBatchSize = 1024
    val mongoConnector = MongoConnector(writeConfig.asOptions) // Gets the connector
    rdd.foreachPartition(iter => if (iter.nonEmpty) {

      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] => // The underlying Java driver MongoCollection instance.

        val updateOptions = new UpdateOptions().upsert(true)

        iter.grouped(DefaultMaxBatchSize).foreach(batch =>

          // Change this block to update / upsert as needed
          batch.map { doc =>
            collection.updateOne(new Document("type", doc.get("type")), new Document("$inc", new Document("count",doc.get("count"))), updateOptions)
          })
      })
    })
  }
  def main(args: Array[String]) {
    val dbName = "keep"
    val mongoDBList = "47.93.227.193:27017"
    val userName = "cloudCompute"
    val passwd = "rtwyyds"
    //设置数据库参数
    val mongo = MongoCon(dbName, mongoDBList, userName, passwd)


    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val lines = ssc.textFileStream(path)
    val readConfigOfEduCount=mongo.produceReadCongfg("live_train")
    val customrdd=sc.loadFromMongoDB(readConfigOfEduCount)
    println(customrdd.first.toJson)


//    ssc.start()
//    ssc.awaitTermination()
  }
//    //统计学历要求
//    val writeConfigOfEduCount=mongo.produceConfg("live_train")
//    val edus = lines.flatMap(line => if (line.split(",").length==20) List(line.split(",")(17)) else List())
//    val edusPairs = edus.map(edu => (edu, 1))
//    val eduResults = edusPairs.reduceByKey((a, b) => a + b)
//    val eduDocument=eduResults.map(eduResult=> {
//      new Document("type",eduResult._1).append("count",eduResult._2)
//    })
//    eduDocument.foreachRDD(rdd=>{
//      saveAndUpdate(rdd,writeConfigOfEduCount);
//    })
//
//    //统计职业需求量
////    val writeConfigOfJobCount=mongo.produceConfg("job_count");
////    val jobNeedPairs=lines.map(line=>(line.split(",")(9),line.split(",")(3).toInt))
////    val jobNeedResults=jobNeedPairs.reduceByKey((a, b) => a + b);
////    val jobNeedDocument=jobNeedResults.map(jobNeedResult=>new Document("type",jobNeedResult._1).append("count",jobNeedResult._2))
////    jobNeedDocument.foreachRDD(rdd=>saveAndUpdate(rdd,writeConfigOfJobCount))
//
//    ssc.start()
//    ssc.awaitTermination()
//  }

}