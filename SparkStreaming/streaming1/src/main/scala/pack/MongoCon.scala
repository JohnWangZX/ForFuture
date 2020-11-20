package pack

import com.mongodb.client.MongoDatabase
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.{ReadConfig, WriteConfig}

case class MongoCon(databaseName: String, mongoDBList: String,userName:String,passwd:String) {
  def produceReadCongfg(collectionName:String):ReadConfig={
    val uri = "mongodb://"+userName+":"+passwd+"@" + mongoDBList +"/"+databaseName+"."+collectionName
    val options = scala.collection.mutable.Map(

      //大多数情况下从primary的replica set读，当其不可用时，从其secondary members读
      "readPreference.name" -> "primaryPreferred",
      "spark.mongodb.input.uri" -> uri,
      "spark.mongodb.output.uri" -> uri,
      "partitioner" ->  "MongoPaginateBySizePartitioner")
    ReadConfig(options)

  }

  def produceWriteConfg(collectionName: String):WriteConfig = {
    val uri = "mongodb://"+userName+":"+passwd+"@" + mongoDBList +"/"+databaseName+"."+collectionName
    val options = scala.collection.mutable.Map(

      //大多数情况下从primary的replica set读，当其不可用时，从其secondary members读
      "readPreference.name" -> "primaryPreferred",

      "spark.mongodb.input.uri" -> uri,
      "spark.mongodb.output.uri" -> uri)
    WriteConfig(options)
  }

}

