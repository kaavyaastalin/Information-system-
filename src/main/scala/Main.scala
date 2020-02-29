
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import com.google.gson.Gson

import scala.collection.JavaConversions._
import scala.io.Source.fromFile

object Main {

  def main(args: Array[String]): Unit = {
    val TOPIC = "household"

    val gson = new Gson
    // PRODUCER
    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](producerProperties)


    // CONSUMER
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("group.id", "dummy-group")
    val consumer = new KafkaConsumer[String, String](consumerProperties)
    consumer.subscribe(Collections.singletonList("household"))


    //while(true) {
      for (us <- readData("HouseholdElectricityFile.csv")) {
      // WRITING
     // val record = new ProducerRecord[String, String]("household", "key", "value")
        println("data==>"+gson.toJson(us))
       val record = new ProducerRecord(TOPIC, "key", gson.toJson(us))
      /*// fire and forget
      producer.send(record)

      // send returns future
      val f = producer.send(record)*/

      // send with callback
      producer.send(record, (m:RecordMetadata, e:Exception) => {
        println
        println("producer callback:")
        println("checksum " + m.checksum())
        println("offset " + m.offset())
        println("partition " + m.partition())
        println("serialized key size " + m.serializedKeySize())
        println("serialized value size " + m.serializedValueSize())
        println("timestamp " + m.timestamp())
        println("topic " + m.topic())
        println("exception" + e)
      }).get(1000L, TimeUnit.MILLISECONDS);


    }

    // READING
    val records = consumer.poll(100)
    for(record:ConsumerRecord[String, String] <- records) {
      println
      println("consumer reading")
      println("checksum " + record.checksum)
      println("key " + record.key)
      println("offset " + record.offset)
      println("partition " + record.partition )
      println("serializedKeySize " + record.serializedKeySize )
      println("serializedValueSize " + record.serializedValueSize )
      println("timestamp " + record.timestamp )
      println("timestampType " + record.timestampType )
      println("topic " + record.topic )
      println("value " + record.value )
    }

    producer.close()
    consumer.close()

  }

  private def readData(fileName: String): List[UserStat] = {
    fromFile(fileName).getLines().drop(1)
      .map(line => line.split(",").map(_.trim))
      .map(parts => UserStat(parts(0), parts(1).toDouble, parts(2).toDouble, parts(3).toDouble,
        parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble, parts(9).toDouble))
      .toList
  }
}