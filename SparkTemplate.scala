
import org.apache.spark.sql.{Column, Encoder, Encoders, SparkSession}
import org.scalatest.{FunSuite, Matchers}

import scala.reflect.ClassTag

class ScalaCountTest extends FunSuite with Matchers{

  test("count"){
   val spark = SparkSession.builder.appName("test").master("local").getOrCreate()
   val sc = spark.sparkContext
   val lines=  sc.textFile("/Users/zrq/workspace/smark/smark-core/src/test/resources/helloworld.txt")
    val strAr =lines.map{line => line.split(',')}
    val words  = strAr.flatMap( strs => strs.toList)
    val wordCountPair = words.map(w=>(w.trim,1))
    val wordReduced = wordCountPair.reduceByKey((a,b)=> a+b)
    val wordCountMap = wordReduced.collectAsMap()
    println(wordCountMap)

  }

  test("csv"){
    case class StageCar(year:String, model:String, make:String)
    case class Car(year:Int,model:String,make:String)
    val spark = SparkSession.builder.appName("test").master("local").getOrCreate()
    val sc = spark.sparkContext
    val file = "/Users/zrq/workspace/smark/smark-core/src/test/resources/cars.csv"
    val cars = spark.read.format("csv").option("header", "true").load(file)
    import spark.implicits._
    implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)

    implicit def tuple2[A1, A2](
                                 implicit e1: Encoder[A1],
                                 e2: Encoder[A2]
                               ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

   /*
    implicit def tuple3[A1, A2, A3](
                                     implicit e1: Encoder[A1],
                                     e2: Encoder[A2],
                                     e3: Encoder[A3]
                                   ): Encoder[(A1,A2,A3)] = Encoders.tuple[A1,A2,A3](e1, e2, e3)
                                   */
    val dsCars = cars.map(row =>StageCar(row.getAs[String]("year"),row.getAs[String]("model"),row.getAs[String]("make")))

    val cleanCars = dsCars.map(scar=> Car( scar.year.toInt,scar.model,scar.make))
    println(cleanCars.collectAsList())

    val carList = dsCars.collectAsList()

    cars.printSchema()

    println(carList)


  }


  test("lookup") {

    val spark = SparkSession.builder.appName("test").master("local").getOrCreate()
    val sc = spark.sparkContext
    val fileOrders = "/Users/zrq/workspace/smark/smark-core/src/test/resources/orders.csv"
    val fileCustomer = "/Users/zrq/workspace/smark/smark-core/src/test/resources/customers.csv"

    val orders = spark.read.format("csv").option("header", "true").load(fileOrders)

    // load the customer  and broadcast it as a lookup map
    val customerRDD = sc.textFile(fileCustomer,1).zipWithIndex().filter(_._2 !=0).map(_._1)
    val strsRDD = customerRDD.map(line => line.split(",")).filter(_.length>=2)
    val idNameRDD = strsRDD.map(strs => (strs(0),strs(1)))
     val broadCustLookupMap = sc.broadcast(idNameRDD.collectAsMap())


    import org.apache.spark.sql.functions._
    import spark.implicits._
    val localCustMap = broadCustLookupMap.value
    println(localCustMap)

    // define a UDF to lookup
    val getCustName = (custId: String) => localCustMap.get(custId).getOrElse("")
    spark.udf.register("get_cust_name", getCustName)

    // Add a new column "custName"
    val ordersWithCustName  = orders.withColumn("custName",expr("get_cust_name(custId)"))
    ordersWithCustName.show(20)

  }
  }
