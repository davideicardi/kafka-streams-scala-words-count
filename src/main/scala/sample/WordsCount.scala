package sample

import java.time.Duration
import java.util.Collections.singletonList
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

object WordsCount extends App {
  import Serdes._

  val INPUT_TOPIC = "text_lines"
  val OUTPUT_TOPIC = "word_count_results"
  val APP_ID = "wordcount-modified"
  val KAFKA_BOOTSTRAPS = "localhost:9092"

  runStreams()

  val producer = runProducer()
  val consumer = runConsumer()

  Await.result(Future.sequence(Seq(producer, consumer)), 10.minutes)

  def runProducer() : Future[Unit] = Future {
    val  props = new Properties()
    props.put("bootstrap.servers", KAFKA_BOOTSTRAPS)

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      val text = scala.io.StdIn.readLine("\n ====== \nEnter some text:  ")
      val record = new ProducerRecord(INPUT_TOPIC, text.hashCode.toString, text)
      producer.send(record)
    }

    producer.close()
  }
  def runConsumer(): Future[Unit] = Future {
    val  props = new Properties()
    props.put("bootstrap.servers", KAFKA_BOOTSTRAPS)

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group")

    val consumer = new KafkaConsumer[String, Long](props)

    consumer.subscribe(singletonList(OUTPUT_TOPIC))

    while (true) {
      val records = consumer.poll(Duration.ofMillis(100))
      if (!records.isEmpty) {
        println("Records found:")
        for (record<-records.asScala){
          println(f"\t${record.key()}=${record.value()}")
        }
      }
    }
  }

  def runStreams(): Unit = {
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAPS)
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100")
      p.put(StreamsConfig.POLL_MS_CONFIG, "100")
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] = builder.stream[String, String](INPUT_TOPIC)
    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count()
    wordCounts.toStream.to(OUTPUT_TOPIC)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }

  }
}