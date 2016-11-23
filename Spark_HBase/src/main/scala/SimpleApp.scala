import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
	val conf = new HBaseConfiguration()
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

	val admin = new HBaseAdmin(conf)

	// list the tables
	val listtables=admin.listTables() 
	listtables.foreach(println)

	val table = new HTable(conf, "vish1")
	val theget= new Get(Bytes.toBytes("L101"))
	val result=table.get(theget)
	val value=result.value()
	println(Bytes.toString(value))

	val logFile = "cs_unique_users/data_dt=201611222334/part-00001" // Should be some file on your system
    	val sconf = new SparkConf().setAppName("Simple Application")
    	val sc = new SparkContext(sconf)
    	val logData = sc.textFile(logFile, 2).cache()
        val userList = logData.collect()

	for (uvid <- userList) {
	     val get = new Get(Bytes.toBytes(uvid))
	     val value = table.get(get).value()
	     println (Bytes.toString(value))
	}

  }
}

// let's insert some data in 'mytable' and get the row
/*
val table = new HTable(conf, "mytable")

val theput= new Put(Bytes.toBytes("rowkey1"))

theput.add(Bytes.toBytes("ids"),Bytes.toBytes("id1"),Bytes.toBytes("one"))
table.put(theput)

val theget= new Get(Bytes.toBytes("rowkey1"))
val result=table.get(theget)
val value=result.value()
println(Bytes.toString(value))
*/
