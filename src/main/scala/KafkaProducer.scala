import java.time.LocalDateTime

import scala.io.Source.fromFile
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.gson.Gson
import org.apache.kafka.clients.producer._

import scala.concurrent.Promise


object KafkaProducer extends App {


  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "household"

  val gson = new Gson

  for (us <- readData("HouseholdElectricityFile.csv")) {

    println("data==>"+gson.toJson(us))
    val record = new ProducerRecord(TOPIC, "key", gson.toJson(us))
    Thread.sleep(50)

    val p = Promise[(RecordMetadata, Exception)]()
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        print("sent record"+metadata.offset())
        p.success((metadata, exception))
      }
    }).get(1000L, TimeUnit.MILLISECONDS);

   /*// producer.send(record ,(metadata, ex)->{

    }).get(1000L, TimeUnit.MILLISECONDS);*/
  }

  producer.close()

  private def readData(fileName: String): List[UserStat] = {
    fromFile(fileName).getLines().drop(1)
      .map(line => line.split(",").map(_.trim))
      .map(parts => UserStat(parts(0), parts(1).toDouble, parts(2).toDouble, parts(3).toDouble,
        parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble, parts(9).toDouble))
      .toList
  }
}