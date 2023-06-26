package org.apache.spark.sql.streaming.sqs

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.event.S3EventNotification
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord
import com.amazonaws.services.sqs.model.{DeleteMessageBatchRequestEntry, GetQueueAttributesRequest, Message, ReceiveMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.util.json.Jackson
import com.amazonaws.{AmazonClientException, AmazonServiceException, ClientConfiguration}
import com.fasterxml.jackson.core.JsonProcessingException
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.struct
import org.apache.spark.util.ThreadUtils

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SqsClient(sourceOptions: SqsSourceOptions,
                hadoopConf: Configuration) extends LazyLogging {
  private val maxMessagesPerBatchSize = 10L
  private val sqsFetchIntervalInSeconds = sourceOptions.fetchIntervalInSeconds
  private val sqsLongPollWaitTimeSeconds = sourceOptions.longPollWaitTimeSeconds
  private val sqsMaxRetries = sourceOptions.maxRetries
  private val totalFilesToFetch = sourceOptions.maxFilesPerTrigger
  private val region = sourceOptions.region
  val sqsUrl = sourceOptions.sqsUrl

  @volatile var exception: Option[Exception] = None
  private val ZDT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private var retriesOnFailure = 0
  private val sqsClient = createSqsClient()

  val sqsScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("sqs-scheduler")

  val sqsFileCache = new SqsFileCache(sourceOptions.maxFileAgeMs, sourceOptions.fileNameOnly)

  val deleteMessageQueue = new java.util.concurrent.ConcurrentLinkedQueue[String]()

  private def sqsFetchMessagesJobFuture[String] = Future {
    try {
      // Fetching messages from Amazon SQS
      val newMessages = sqsFetchMessages()
      // Filtering the new messages which are already not seen
      if (newMessages.nonEmpty) {
        logger.debug(s"newMessages size is ${newMessages.size}")
        newMessages.filter(message => sqsFileCache.isNewFile(message._1))
          .foreach(message =>
            sqsFileCache.add(message._1, MessageDescription(message._2, false, message._3)))
      }
      "Success"
    } catch {
      case e: Exception =>
        exception = Some(e)
        "Failure"
    }
  }

  sqsScheduler.scheduleWithFixedDelay(
    // this pattern uses pool to submit sqs requests asap, exhaust all thread, otherwise it was causing one req at a time
    // it fills up the cache which is used by SqsSource Dataframe generation
    () => {
      var totalFilesNeeded = totalFilesToFetch.get.toLong
      val soFarUncommittedFiles = sqsFileCache.getUncommittedFiles(Int.MaxValue).size
      logger.info(s"totalFilesNeeded is set to: ${totalFilesNeeded} and " +
        s"soFarUncommittedFiles is ${soFarUncommittedFiles} and Cache Map size is ${sqsFileCache.size}")
      //if we already have enough messages
      if(soFarUncommittedFiles < (3*totalFilesNeeded)) {
        // get sqs stats
        val sqsStats = getSQSStats();
        sqsStats.foreach({ keyVal => logger.debug(s"Sqs stats [k,v] pairs:  ${keyVal._1}=${keyVal._2}") })
        val numOfSQSReq = totalFilesNeeded / maxMessagesPerBatchSize // each sqs req max files it can get is 10

        // we keep 3 x the totalFilesNeeded to buffer
        val numOfSQSNotifications = sqsStats.get("ApproximateNumberOfMessages").get.toLong
        if (totalFilesNeeded > numOfSQSNotifications) {
          totalFilesNeeded = numOfSQSNotifications;
        }
        logger.info(s"numOfSQSNotifications available is sqs: ${numOfSQSNotifications}")
        //build all numOfSQSReq futures
        val sqsCallsFutures = List.fill(numOfSQSReq.toInt)(sqsFetchMessagesJobFuture)
        // wait for 1mints to all calls to finish
        Future.sequence(sqsCallsFutures)
        logger.debug(s"Waiting for ${numOfSQSReq} calls to finish")
      }
    },0, sqsFetchIntervalInSeconds, TimeUnit.SECONDS)

  private def getSQSStats(): Map[String, String] = {
    sqsClient.getQueueAttributes(new GetQueueAttributesRequest(sqsUrl, List("All").asJava)).getAttributes().asScala.toMap
  }

  private def sqsFetchMessages(): Seq[(String, Long, String)] = {
    val messageList = try {
      val receiveMessageRequest = new ReceiveMessageRequest()
        .withQueueUrl(sqsUrl)
        .withWaitTimeSeconds(sqsLongPollWaitTimeSeconds)
        .withMaxNumberOfMessages(maxMessagesPerBatchSize.toInt)
        .withVisibilityTimeout(1800)// hide messages for 30m
      val messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages.asScala
      retriesOnFailure = 0
      logger.debug(s"successfully received ${messages.size} messages")
      messages
    } catch {
      case ase: AmazonServiceException =>
        val message =
          """
            |Caught an AmazonServiceException, which means your request made it to Amazon SQS,
            | rejected with an error response for some reason.
        """.stripMargin
        logger.warn(message)
        logger.warn(s"Error Message: ${ase.getMessage}")
        logger.warn(s"HTTP Status Code: ${ase.getStatusCode}, AWS Error Code: ${ase.getErrorCode}")
        logger.warn(s"Error Type: ${ase.getErrorType}, Request ID: ${ase.getRequestId}")
        evaluateRetries()
        List.empty
      case ace: AmazonClientException =>
        val message =
          """
            |Caught an AmazonClientException, which means, the client encountered a serious
            | internal problem while trying to communicate with Amazon SQS, such as not
            |  being able to access the network.
        """.stripMargin
        logger.warn(message)
        logger.warn(s"Error Message: ${ace.getMessage()}")
        evaluateRetries()
        List.empty
      case e: Exception =>
        val message = "Received unexpected error from SQS"
        logger.warn(message)
        logger.warn(s"Error Message: ${e.getMessage()}")
        evaluateRetries()
        List.empty
    }
    if (messageList.nonEmpty) {
      parseSqsMessages(messageList)
    } else {
      Seq.empty
    }
  }

  private def parseSqsMessages(messageList: Seq[Message]): Seq[(String, Long, String)] = {
    val errorMessages = scala.collection.mutable.ListBuffer[String]()
    var returnMsgSeq = Seq[(String, Long, String)]()
    messageList.foreach(message => {
      try {
        // get first handle to remove later from queue
        val messageReceiptHandle = message.getReceiptHandle
        //  parse body to get bucket name out
        val body = Jackson.stringMapFromJsonString(message.getBody)
        if(body.get("Message") != null) {
          val messageStr: String = body.get("Message")
          val notification: S3EventNotification = S3EventNotification.parseJson(messageStr)

          val records: Seq[S3EventNotificationRecord] = notification.getRecords.asScala
          returnMsgSeq = returnMsgSeq ++ records // only newly object created events
            .filter(event => "ObjectCreated:Put".equalsIgnoreCase(event.getEventName))
            .map(record => (s"s3a://${record.getS3.getBucket.getName}/${record.getS3.getObject.getUrlDecodedKey}",
              convertTimestampToMills(record.getEventTime.toString),
              messageReceiptHandle))
        }
      } catch {
        case jpe: JsonProcessingException => {
          errorMessages.append(message.getReceiptHandle)
          logger.warn(s"Unexpected JsonProcessingException while parsing SQS message " +
            s"${message.getBody}  Exception ${jpe.getMessage}")
        }
        case ex: Exception => {
          errorMessages.append(message.getReceiptHandle)
          logger.warn(s"Unexpected Exception while parsing SQS message " +
            s"${message.getBody}  Exception ${ex.getMessage}")
        }
      }
    })
    if (errorMessages.nonEmpty) {
      addToDeleteMessageQueue(errorMessages.toList)
    }
    returnMsgSeq
  }

  private def convertTimestampToMills(timestamp: String): Long = {
    LocalDateTime.parse(timestamp, ZDT_FORMATTER).atZone(ZoneId.of("UTC")).toInstant.toEpochMilli
  }

  private def evaluateRetries(): Unit = {
    retriesOnFailure += 1
    if (retriesOnFailure >= sqsMaxRetries) {
      logger.error("Max retries reached")
      exception = Some(new SparkException("Unable to receive Messages from SQS for " +
        s"${sqsMaxRetries} times Giving up. Check logs for details."))
    } else {
      logger.warn(s"Attempt ${retriesOnFailure}." +
        s"Will reattempt after ${sqsFetchIntervalInSeconds} seconds")
    }
  }

  private def createSqsClient(): AmazonSQS = {
    try {
      val isClusterOnEc2Role = hadoopConf.getBoolean(
        "fs.s3.isClusterOnEc2Role", false) || hadoopConf.getBoolean(
        "fs.s3n.isClusterOnEc2Role", false) || sourceOptions.useInstanceProfileCredentials
      if (!isClusterOnEc2Role) {
        val accessKey = hadoopConf.getTrimmed("fs.s3a.access.key")
        val secretAccessKey = new String(hadoopConf.getPassword("fs.s3a.secret.key")).trim
        val sessionToken = new String(hadoopConf.getPassword("fs.s3a.session.token")).trim
        logger.info("Using credentials from keys provided")
        val basicAwsCredentialsProvider = new BasicSessionCredentialsProvider(accessKey, secretAccessKey, sessionToken);
        AmazonSQSClientBuilder
          .standard()
          // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-throughput-horizontal-scaling-and-batching.html
          .withClientConfiguration(new ClientConfiguration()
            .withMaxConnections(totalFilesToFetch.get.toInt /maxMessagesPerBatchSize.toInt)
            .withTcpKeepAlive(true))
          .withCredentials(basicAwsCredentialsProvider)
          .withRegion(region)
          .build()
      } else {
        logger.info("Using the credentials attached to the instance")
        //val instanceProfileCredentialsProvider = new InstanceProfileCredentialsProviderWithRetries()
        AmazonSQSClientBuilder
          .standard()
          .withClientConfiguration(new ClientConfiguration()
            .withMaxConnections(totalFilesToFetch.get / maxMessagesPerBatchSize.toInt)
            .withTcpKeepAlive(true))
          .withRegion(region)
          .withCredentials(new ProfileCredentialsProvider())
          .build()
      }
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error occured while creating Amazon SQS Client", e)
    }
  }

  def addToDeleteMessageQueue(messageReceiptHandles: List[String]): Unit = {
    deleteMessageQueue.addAll(messageReceiptHandles.asJava)
  }

  def deleteMessagesFromQueue(): Unit = {
    var messageReceiptHandles = List[String]()
    try {
      var count = -1
      messageReceiptHandles = deleteMessageQueue.asScala.toList
      val messageGroups = messageReceiptHandles.sliding(10, 10).toList
      messageGroups.foreach { messageGroup =>
        val requestEntries = messageGroup.foldLeft(List[DeleteMessageBatchRequestEntry]()) {
          (list, messageReceiptHandle) =>
            count = count + 1
            list :+ new DeleteMessageBatchRequestEntry(count.toString, messageReceiptHandle)
        }.asJava
        logger.debug(s"Deleted ${requestEntries.size()} committed messages from SQS")
        val batchResult = sqsClient.deleteMessageBatch(sqsUrl, requestEntries)
        if (!batchResult.getFailed.isEmpty) {
          batchResult.getFailed.asScala.foreach { entry =>
            sqsClient.deleteMessage(
              sqsUrl, requestEntries.get(entry.getId.toInt).getReceiptHandle)
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Unable to delete message from SQS ${e.getMessage}")
    }
    logger.info(s"Deleted ${messageReceiptHandles.size} committed messages from SQS")
    deleteMessageQueue.clear()
  }

  def assertSqsIsWorking(): Unit = {
    if (exception.isDefined) {
      throw exception.get
    }
  }

}