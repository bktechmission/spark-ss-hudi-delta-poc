package org.apache.spark.sql.streaming.sqs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * A custom hash map used to track the list of files seen. This map is thread-safe.
 * To prevent the hash map from growing indefinitely, a purge function is available to
 * remove files "maxAgeMs" older than the latest file.
 */

class SqsFileCache(maxAgeMs: Long, fileNameOnly: Boolean) extends LazyLogging {
  require(maxAgeMs >= 0)
  if (fileNameOnly) {
    logger.warn("'fileNameOnly' is enabled. Make sure your file names are unique (e.g. using " +
      "UUID), otherwise, files with the same name but under different paths will be considered " +
      "the same and causes data lost.")
  }

  /** Mapping from file path to its message description. */
  private val sqsMap = new ConcurrentHashMap[String, MessageDescription]

  /** Timestamp for the last purge operation. */
  private var lastPurgeTimestamp: Long = -1L

  /** Timestamp of the latest file. */
  private var latestTimestamp: Long = 0L

  @inline private def stripPathIfNecessary(path: String) = {
    if (fileNameOnly) new Path(new URI(path)).getName else path
  }

  /**
   * Returns true if we should consider this file a new file. The file is only considered "new"
   * if it is new enough that we are still tracking, and we have not seen it before.
   */
  def isNewFile(path: String): Boolean = {
    !sqsMap.containsKey(stripPathIfNecessary(path))
  }

  /** Add a new file to the map. */
  def add(path: String, fileStatus: MessageDescription): Unit = {
    sqsMap.put(stripPathIfNecessary(path), fileStatus)
    if (fileStatus.timestamp > latestTimestamp) {
      latestTimestamp = fileStatus.timestamp
    }
  }

  /**
   * Returns all the new files found - ignore aged files and files that we have already seen.
   * Sorts the files by timestamp.
   */
  def getUncommittedFiles(maxFilesPerTrigger: Int): List[(String, Long, String)] = {
    val iterator = sqsMap.asScala.iterator
    val uncommittedFiles = ListBuffer[(String, Long, String)]()
    var maxNumFiles = Int.MaxValue
    if(maxFilesPerTrigger != null) {
      maxNumFiles = maxFilesPerTrigger
    }
    while (uncommittedFiles.length < maxNumFiles && iterator.hasNext) {
      val file = iterator.next()
      if (!file._2.isCommitted && file._2.timestamp < lastPurgeTimestamp) {
        logger.warn(s"FILE DROPPING ALERT! : getUncommittedFiles Found File ${file._1} with Timestamp ${file._2.timestamp} greater then lastPurgeTimestamp : ${lastPurgeTimestamp}")
      }
      if (!file._2.isCommitted && file._2.timestamp >= lastPurgeTimestamp) {
        uncommittedFiles += ((file._1, file._2.timestamp, file._2.messageReceiptHandle))
      }
    }
    uncommittedFiles.toList
  }

  /** Removes aged entries and returns the number of files removed. */
  def purge(): Int = {
    lastPurgeTimestamp = latestTimestamp - maxAgeMs
    var count = 0
    sqsMap.asScala.foreach { fileEntry =>
      // only purge committed files
      if (fileEntry._2.timestamp < lastPurgeTimestamp || fileEntry._2.isCommitted) {
        logger.debug(s"removed file :  ${fileEntry._1} with timestamp: ${fileEntry._2.timestamp}")
        sqsMap.remove(fileEntry._1)
        count += 1
      }
    }
    logger.debug(s"# of purged files :  ${count}")
    count
  }

  /** Mark file entry as committed or already processed */
  def markCommitted(path: String): Unit = {
    sqsMap.replace(path, MessageDescription(
      sqsMap.get(path).timestamp, true, sqsMap.get(path).messageReceiptHandle))
  }

  def size: Int = sqsMap.size()

}

/**
 * A case class to store file metadata. Metadata includes file timestamp, file status -
 * committed or not committed and message reciept handle used for deleting message from
 * Amazon SQS
 */
case class MessageDescription(timestamp: Long,
                              isCommitted: Boolean = false,
                              messageReceiptHandle: String)
