package com.calvin.http4s.kafka

import java.nio.channels.AsynchronousChannelGroup
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import cats.implicits._
import cats.syntax._
import cats.effect.{Effect, IO, Sync}
import fs2.{Scheduler, Stream, StreamApp}
import org.http4s.server.blaze.BlazeBuilder
import com.calvin.http4s.kafka.proto._
import com.calvin.http4s.kafka.proto.account._
import scodec.bits.ByteVector
import spinoco.fs2.kafka._
import spinoco.protocol.kafka._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

object HelloWorldServer extends StreamApp[IO] {
  import scala.concurrent.ExecutionContext.Implicits.global

  def kafkaFs2Producer[F[_]: Effect](scheduler: Scheduler, kafkaClient: KafkaClient[F]): Stream[F, Unit] = {
    scheduler.awakeDelay(1.second).evalMap { _ =>
      val kId = ByteVector(KID(UUID.randomUUID.toString()).toByteArray.toVector)
      val kBody = ByteVector(KBody(UUID.randomUUID.toString(), " Test Message").toByteArray.toVector)
      kafkaClient.publish1(
        topicId = topic("my-topic-1"),
        partition = partition(0),
        key = kId,
        message = kBody,
        requireQuorum = true,
        serverAckTimeout = 10 seconds).void
    }
  }

  def kafkaFs2Consumer[F[_]: Effect](kafkaClient: KafkaClient[F]): Stream[F, KID] = {
    kafkaClient.subscribe(
      topicId = topic("my-topic-1"),
      partition = partition(0),
      offset = offset(0),
      prefetch = false,
      minChunkByteSize = 0,
      maxChunkByteSize = 1024,
      maxWaitTime = 30.seconds,
      leaderFailureTimeout = 30.seconds,
      leaderFailureMaxAttempts = 10
    ).map[KID]{t =>
        println(t.key.toArray)
      val kid = KID.parseFrom(t.key.toArray)
      println(kid)
      kid
    }
  }




  def stream(args: List[String], requestShutdown: IO[Unit]) = ServerStream.stream[IO]
}

object ServerStream {

  def helloWorldService[F[_]: Effect] = new HelloWorldService[F].service

  def stream[F[_]: Effect](implicit ec: ExecutionContext) =
    for {
      scheduler <- Scheduler[IO](5)
      logger <- Stream.eval(Logger.JDKLogger[IO](java.util.logging.Logger.getGlobal()))
      ag <- Stream.eval(IO(AsynchronousChannelGroup.withThreadPool(Executors.newCachedThreadPool())))
      kafkaClient <- KafkaClient.apply[IO](ensemble = Set(broker("localhost", port = 9092)), protocol = ProtocolVersion.Kafka_0_10_2, clientName = "kafka-fs2")(AG = ag, S = scheduler, EC = ec, F = Effect[IO], Logger = logger)
      s <- BlazeBuilder[IO]
          .bindHttp(8080, "0.0.0.0")
          .mountService(helloWorldService, "/")
          .serve concurrently HelloWorldServer.kafkaFs2Producer[IO](scheduler, kafkaClient)
    } yield s
}
