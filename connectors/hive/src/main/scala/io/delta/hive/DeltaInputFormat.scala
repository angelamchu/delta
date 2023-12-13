/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.hive

import java.io.IOException
import java.net.URI

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.io.{ArrayWritable, NullWritable}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.parquet.hadoop.ParquetInputFormat
import org.slf4j.LoggerFactory

/**
 * A special [[InputFormat]] to wrap [[ParquetInputFormat]] to read a Delta table.
 *
 * The underlying files in a Delta table are in Parquet format. However, we cannot use the existing
 * [[ParquetInputFormat]] to read them directly because they only store data for data columns.
 * The values of partition columns are in Delta's metadata. Hence, we need to read them from Delta's
 * metadata and re-assemble rows to include partition values and data values from the raw Parquet
 * files.
 *
 * Note: We cannot use the file name to infer partition values because Delta Transaction Log
 * Protocol requires "Actual partition values for a file must be read from the transaction log".
 *
 * In the current implementation, when listing files, we also read the partition values and put them
 * into an `Array[PartitionColumnInfo]`. Then create a temp `Map` to store the mapping from the file
 * path to `PartitionColumnInfo`s. When creating an [[InputSplit]], we will create a special
 * [[FileSplit]] called [[DeltaInputSplit]] to carry over `PartitionColumnInfo`s.
 *
 * For each reader created from a [[DeltaInputSplit]], we can get all partition column types, the
 * locations of a partition column in the schema, and their string values. The reader can build
 * [[org.apache.hadoop.io.Writable]] for all partition values, and insert them to the raw row
 * returned by [[org.apache.parquet.hadoop.ParquetRecordReader]].
 */
class DeltaInputFormat(realInput: ParquetInputFormat[ArrayWritable])
  extends FileInputFormat[NullWritable, ArrayWritable] {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaInputFormat])

  /**
   * A temp [[Map]] to store the path uri and its partition information. We build this map in
   * `listStatus` and `makeSplit` will use it to retrieve the partition information for each split.
   * */
  private var fileToPartition: Map[URI, Array[PartitionColumnInfo]] = Map.empty

  def this() {
    this(new ParquetInputFormat[ArrayWritable](classOf[DataWritableReadSupport]))
  }

  override def getRecordReader(
      split: InputSplit,
      job: JobConf,
      reporter: Reporter): RecordReader[NullWritable, ArrayWritable] = {
    split match {
      case deltaSplit: DeltaInputSplit =>
        new DeltaRecordReaderWrapper(this.realInput, deltaSplit, job, reporter)
      case _ =>
        throw new IllegalArgumentException("Expected DeltaInputSplit but it was: " + split)
    }
  }

  @throws(classOf[IOException])
  override def listStatus(job: JobConf): Array[FileStatus] = {
    checkHiveConf(job)
    val deltaRootPath = new Path(job.get(DeltaStorageHandler.DELTA_TABLE_PATH))
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), Array(deltaRootPath), job)
    val (files, partitions) =
      try {
        DeltaHelper.listDeltaFiles(deltaRootPath, job)
      } catch {
        // Hive is using Java Reflection to call `listStatus`. Because `listStatus` doesn't declare
        // `MetaException`, the Reflection API would throw `UndeclaredThrowableException` without an
        // error message if `MetaException` was thrown directly. To improve the user experience, we
        // wrap `MetaException` with `IOException` which will provide a better error message.
        case e: MetaException => throw new IOException(e)
      }
    fileToPartition = partitions.filter(_._2.nonEmpty)
    files
  }

  private def checkHiveConf(job: JobConf): Unit = {
    val engine = HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE)
    val deltaFormat = classOf[HiveInputFormat].getName
    engine match {
      case "mr" =>
        if (HiveConf.getVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT) != deltaFormat) {
          throw deltaFormatError(engine, HiveConf.ConfVars.HIVEINPUTFORMAT.varname, deltaFormat)
        }
      case "tez" =>
        if (HiveConf.getVar(job, HiveConf.ConfVars.HIVETEZINPUTFORMAT) != deltaFormat) {
          throw deltaFormatError(engine, HiveConf.ConfVars.HIVETEZINPUTFORMAT.varname, deltaFormat)
        }
      case other =>
        throw new UnsupportedOperationException(s"The execution engine '$other' is not supported." +
          s" Please set '${HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname}' to 'mr' or 'tez'")
    }
  }

  private def deltaFormatError(
      engine: String,
      formatConfig: String,
      deltaFormat: String): Throwable = {
    val message =
      s"""'$formatConfig' must be set to '$deltaFormat' when reading a Delta table using
         |'$engine' execution engine. You can run the following SQL command in Hive CLI
         |before reading a Delta table,
         |
         |> SET $formatConfig=$deltaFormat;
         |
         |or add the following config to the "hive-site.xml" file.
         |
         |<property>
         |  <name>$formatConfig</name>
         |  <value>$deltaFormat</value>
         |</property>
      """.stripMargin
    new IllegalArgumentException(message)
  }

  override def makeSplit(
      file: Path,
      start: Long,
      length: Long,
      hosts: Array[String]): FileSplit = {
    new DeltaInputSplit(
      file,
      start,
      length,
      hosts,
      fileToPartition.getOrElse(file.toUri, Array.empty))
  }

  override def makeSplit(
      file: Path,
      start: Long,
      length: Long,
      hosts: Array[String],
      inMemoryHosts: Array[String]): FileSplit = {
    new DeltaInputSplit(
      file,
      start,
      length,
      hosts,
      inMemoryHosts,
      fileToPartition.getOrElse(file.toUri, Array.empty))
  }

  // There is a bug in the original HiveInputFormat class and its associated classes that will
  // take even a tiny kb Parquet file and split it into 8 splits, each split then being
  // assigned to a mapper.  This creates what a customer appropriately described as
  // a "mapper explosion", causing a very large amount of infrastructure to be used to execute
  // a query on Delta tables.  Today new classes are in use by native formats such as Parquet and
  // ORC, and the new classes do not have the excessive mapper problem.
  // The bug has nothing to do with this connector, it was a bug
  // present in the original set of Hive classes that this connector is based off of.
  // The new classes are very different then the old, and an extensive rewrite would be required
  // to make this connector use those new classes.
  // The below makes the old Hive classes stop creating a ton of splits for no reason.
  // Hard-coding numSplits to 1 here will force it to default the split size to the system's default
  // block size.  If a file is smaller than one block then only one split will be created
  // for it.  Unlike the new CombineFileInputFormat class, FileInputFormat will not combine
  // files in the same split, only split up individual files.  So this connector will create
  // a few more splits than the new Hive classes do, since the new classes will combine multiple
  // files into the same split.
  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    val splits = super.getSplits(job, 1)
    // Reset the temp [[Map]] to release the memory
    fileToPartition = Map.empty
    splits
  }


  // In my testing, the newer Hive classes seem to aim for a split size of around 256mb.
  // The old Hive classes will use the default block size on the Hive system, which
  // is usually 64mb or 128mb.  That will still create too many splits vs. querying native
  // ORC or Parquet, so here we're overriding computeSplitSize to force the split size to be 256mb.
  // To optimize your Delta table for number of mappers when querying with this connector,
  // set the file size target on your Delta tables to 256mb or larger.
  override protected def computeSplitSize(goalSize: Long, minSize: Long, blockSize: Long): Long = {
    val splitSize = super.computeSplitSize(goalSize, minSize, 268435456)
    splitSize
  }


}
