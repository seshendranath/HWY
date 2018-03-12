import org.apache.hadoop.fs.{FileContext, FileSystem}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import com.amazonaws.services.s3.model._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import scala.collection.JavaConversions._

spark.sparkContext.setLogLevel("warn")

val dfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val dfc = FileContext.getFileContext(spark.sparkContext.hadoopConfiguration)
val hiveMetaStore = new HiveMetaStoreClient(new HiveConf())
val s3 = AmazonS3ClientBuilder.defaultClient()
val tx = TransferManagerBuilder.defaultTransferManager

object util extends Utility
