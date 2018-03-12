import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.sql.ResultSet
import javax.sql.rowset.CachedRowSet
import com.sun.rowset.CachedRowSetImpl
import java.text.SimpleDateFormat


val serverip = "..." // Live
val user = "aeservices"
val pwd = "..."
val url = "jdbc:sqlserver://"+serverip+":1433;database=DW_facts"

def executeUpdate(query: String): Int = {

        val conn = DriverManager.getConnection(url, user, pwd)
        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

        var rs: Int = -1

        try {
            rs = statement.executeUpdate(query)

        } catch {
            case e: Exception => errorHandler(e)
            case e: Error => errorHandler(e)

        } finally {
            conn.close()
        }

        def errorHandler(e: Throwable): Unit = {
            println(s"Something Went Wrong while executing the Query $query")
            println(e.printStackTrace())
            System.exit(1)
        }

        rs
    }


val props = new Properties()
props.put("user", user)
props.put("password", pwd)
props.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")



val startdate = "2016-04-30"
val enddate = "2018-02-05"

val sdf = new SimpleDateFormat("yyyy-MM-dd")
val cal = Calendar.getInstance()
cal.setTime(sdf.parse(startdate))
val calendarEndTime = sdf.parse(enddate)

def load(): Int = {
    while (cal.getTimeInMillis < calendarEndTime.getTime) 
    {

    cal.set(Calendar.DATE,1)
    cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DAY_OF_MONTH))
    val calEndTimeStaring = sdf.format(cal.getTime)

    val res = executeUpdate(s"DELETE FROM DW_FACTS.dbo.SearchMetricsSnapshotByStayDate WHERE snapshotdate = '$calEndTimeStaring'")
    println(s"DELETED $res rows from SearchMetricsSnapshotByStayDate for snapshotdate '$calEndTimeStaring'")

    val q = s"""SELECT SnapshotDate
          ,StrategicdestinationUUID
          ,StayDate
          ,uniqueDatedSearch DatedSearchCount
          ,ArrivalDate
          ,DepartureDate
          ,ArrivalDay
          ,DepartureDay
          ,StayDay FROM djayswal.staytable WHERE snapshotdate = '$calEndTimeStaring'"""

    println(q)
    println("=======")

    println("Writing Data to Sql Server")
    spark.sql(q).write.mode(SaveMode.Append).jdbc(url, "SearchMetricsSnapshotByStayDate", props)

    cal.add(Calendar.DATE, 5)
    Thread.sleep(30000)

    }

    1
}


@annotation.tailrec
final def retry(n: Int)(fn: => Int): util.Try[Int] = {
 util.Try { fn } match {
   case x: util.Success[Int] => x
   case util.Failure(e) => { println(e); println("RETRYING"); if (n > 1) retry(n - 1)(fn) else util.Try(1) }
 }
}

retry(3)(load)
