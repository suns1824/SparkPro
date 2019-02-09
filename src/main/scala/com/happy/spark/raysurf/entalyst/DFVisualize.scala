package com.happy.spark.raysurf.entalyst

//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{Logging, SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, Row, SaveMode, _}
//import com.alibaba.fastjson.{JSON, JSONObject}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.sql.types.StringType
//
//import scala.collection.mutable.ArrayBuffer
/**
  * 功能：对hive表的列信息进行统计。
  * 统计结果包括：
  * 1.包含每列的平均值，中位数，最小值最大值，方差，唯一值，缺失值，列类型。
  * 2.列的直方图分布（字符串top10，数值列10个区间），四分位图分布（数值列）。
  *
  * 实现逻辑：
  * 1.利用spark的describe函数获取到最大值，最小值，均值，方差等。
  * 2。利用sql：获取唯一值及缺失值，sql样例如下：
  *   select count(distinct(id)) as unique_id , count(distinct(name)) as unique_name, sum(case when id is null then 1 else 0 end) as missing_id, sum(case when name is null then 1 else 0 end) as missing_name, sum(1) as totalrows from zpcrcf
  *   结果：
  *   +---------+-----------+----------+------------+---------+
  *   |unique_id|unique_name|missing_id|missing_name|totalrows|
  *   +---------+-----------+----------+------------+---------+
  *   |       14|         12|         0|           0|       14|
  *   +---------+-----------+----------+------------+---------+
  *
  * 3.利用sql：获取四分位图，sql样例如下；
  *    select 'Quartile_id' as  colName, ntil, max(id) as  num from (select id,  ntile(4) OVER (order by id)as ntil from zpcrcf) tt group by ntil
  *    结果：
  *    +------------+----+---+
  *    |     colName|ntil|num|
  *    +------------+----+---+
  *    | Quartile_id|   1|  3|
  *    | Quartile_id|   2|  7|
  *    | Quartile_id|   3| 14|
  *    | Quartile_id|   4|100|
  *
  * 4.数值型直方图10阶段分区间，通过最大值减最小值，获取各个区间内的分布。
  *   sql样例如下：
  *   select 'MathHistogram_age' as colName, partNum, count(1) as num from ( select age, (case  when (age >= 29.0 and age <= 36.1) then 1  when (age > 36.1 and age <= 43.2) then 2  when (age > 43.2 and age <= 50.3) then 3  when (age > 50.3 and age <= 57.4) then 4  when (age > 57.4 and age <= 64.5) then 5  when (age > 64.5 and age <= 71.6) then 6  when (age > 71.6 and age <= 78.69999999999999) then 7  when (age > 78.69999999999999 and age <= 85.8) then 8  when (age > 85.8 and age <= 92.9) then 9  when (age > 92.9 and age <= 100.0) then 10  else 0  end ) as partNum from zpcrcf) temptableScala group by partNum
  *   结果：
  *   +-----------------+-------+---+
  *   |          colName|partNum|num|
  *   +-----------------+-------+---+
  *   |MathHistogram_age|      0|  1|
  *   |MathHistogram_age|      1|  3|
  *   |MathHistogram_age|     10| 10|
  *   | MathHistogram_id|      1| 10|
  *   | MathHistogram_id|      2|  3|
  *   | MathHistogram_id|     10|  1|
  *   +-----------------+-------+---+
  *
  *
  *
  * Created by zpc on 2016/4/26.
  */
//object DataFrameVisiualize extends Logging {
//
//  def runforstatistic(hiveContext: HiveContext, params: JSONObject) = {
//    val arr = params.getJSONArray("targetType")
//    var i = 0
//    while( arr != null && i < arr.size()){
//      val obj = arr.getJSONObject(i)
//      if("dataset".equalsIgnoreCase(obj.getString("targetType"))){
//        val tableNameKey = obj.getString("targetName")
//        val tableName = params.getString(tableNameKey)
//        val user = params.getString("user")
//        run(hiveContext, tableName, user)
//      }
//      i = i+1
//    }
//  }
//
//  def run(hiveContext: HiveContext, tableName: String, user: String) = {
//    val pathParent = s"/user/$user/mlaas/tableStatistic/$tableName"
//    //    val conf = new SparkConf().setAppName("DataFrameVisiualizeJob")
//    //    val sc = new SparkContext(conf)
//    //    val hiveContext = new HiveContext(sc)
//    //    val sqlContext = new SQLContext(sc)
//    //0.获取DB的schema信息
//    val schemadf = hiveContext.sql("desc " + tableName)
//    //schema信息落地
//    val filePathSchema = pathParent + "/schemajson"
//    schemadf.write.mode(SaveMode.Overwrite).format("json").save(filePathSchema)
//
//    //1.加载表到dataframe
//    val df = hiveContext.sql("select * from " + tableName)
//    //2.获取dataframe的describe信息，默认为获取到的都为数值型列
//    val dfdesc = df.describe()
//    //    //3.描述信息落地
//    //    val filePath = pathParent + "/describejson"
//    //    des.write.mode(SaveMode.Overwrite).format("json").save(filePath)
//    //    val dfdesc = sqlContext.read.format("json").load(filePath)
//
//    //4.列信息区分为mathColArr 和 strColArr
//    val mathColArr = dfdesc.columns.filter(!_.equalsIgnoreCase("summary"))
//    val (colMin, colMax, colMean, colStddev, colMedian) = getDesfromDF(dfdesc, mathColArr)
//    val allColArr = df.columns
//
//    // col type 存在vector类型,此处仅统计string和num类型的
//    val typeMap = df.dtypes.toMap
//    val strColArr = allColArr.filter(typeMap.get(_).get.equals(StringType.toString))
//    //    val strColArr = allColArr.filter(!_.equalsIgnoreCase("summary")).diff(mathColArr)
//
//
//    saveRecords(hiveContext, tableName, 100, pathParent + "/recordsjson")
//    val jsonobj = getAllStatistics(hiveContext, tableName, allColArr, strColArr, mathColArr, 10, colMin, colMax)
//
//    jsonobj.put("colMin", colMin)
//    jsonobj.put("colMax", colMax)
//    jsonobj.put("colMean", colMean)
//    jsonobj.put("colStddev", colStddev)
//    jsonobj.put("colMedian", colMedian)
//
//    val jsonStr = jsonobj.toString
//    val conf1 = new Configuration()
//    val fs = FileSystem.get(conf1)
//    val fileName = pathParent + "/jsonObj"
//    val path = new Path(fileName)
//    val hdfsOutStream = fs.create(path)
//    hdfsOutStream.write(jsonStr.getBytes("utf-8"))
//    hdfsOutStream.flush()
//    hdfsOutStream.close()
//    //    fs.close();
//
//  }
//
//  def saveRecords(hiveContext: HiveContext, tableName: String, num: Int, filePath: String) : Unit = {
//    hiveContext.sql(s"select * from $tableName limit $num").write.mode(SaveMode.Overwrite).format("json").save(filePath)
//  }
//  /**
//    * 根据allCols， mathColArr， strColArr 三个数组，返回带有所有统计信息（除去已经根据describe获取到的）的dataframes。
//    * 返回的dataframe结果进行遍历，填充各个属性的值。
//    */
//  def getAllStatistics(hiveContext: HiveContext, tableName: String, allColArr: Array[String], strColArr: Array[String], mathColArr: Array[String], partNum: Int, colMin: java.util.HashMap[String, Double], colMax: java.util.HashMap[String, Double]) :
//  JSONObject = {
//    val jsonobj = new JSONObject()
//    val sb = new StringBuffer()
//    sb.append("select ")
//    allColArr.map{col => sb.append(s"count(distinct(`$col`)) as unique_$col ," +
//      s"sum(case when `$col` is null then 1 else 0 end) as missing_$col, ")}
//    sb.append(s"sum(1) as totalrows from $tableName")
//    val df = hiveContext.sql(sb.toString)
//    val colUnique = new java.util.HashMap[String, Long]//唯一值
//    val colMissing = new java.util.HashMap[String, Long]//缺失值
//    var totalrows = 0L
//    df.take(1).foreach(row => (totalrows = row.getAs[Long]("totalrows"), jsonobj.put("totalrows", totalrows) ,allColArr.foreach(col => (colUnique.put(col, row.getAs[Long]("unique_"+col)),colMissing.put(col, row.getAs[Long]("missing_"+col))) ) ))
//
//    val dfArr = ArrayBuffer[DataFrame]()
//    val strHistogramSql = new StringBuffer()
//    strHistogramSql.append(s"""
//         SELECT tta.colName, tta.value, tta.num
//         FROM (
//         SELECT ta.colName, ta.value, ta.num, ROW_NUMBER() OVER (PARTITION BY ta.colName ORDER BY ta.num DESC) AS row
//         FROM (
//      """)
//
//    var vergin = 0
//    for(col <- strColArr){
//      if(vergin == 1){
//        strHistogramSql.append(" UNION ALL ")
//      }
//      vergin = 1
//      strHistogramSql.append(s"""
//      SELECT 'StrHistogram_$col' AS colName, `$col` AS value, COUNT(1) AS num
//      FROM $tableName
//        GROUP BY `$col` """)
//    }
//    strHistogramSql.append(s"""
//      ) ta
//      ) tta
//      WHERE tta.row <= $partNum
//      """)
//    //整个表中，可能不存在字符串型的列。此时，sql是不完整的,添加到df中会报错。
//    if(strColArr != null && strColArr.size != 0 ){
//      val dfStrHistogram =  hiveContext.sql(strHistogramSql.toString)
//      dfArr.append(dfStrHistogram)
//    }
//
//    for(col <- mathColArr) {
//      val df1 = hiveContext.sql(s"select 'Quartile_$col' as  colName, ntil, bigint(max(`$col`)) as  num from (select `$col`,  ntile(4) OVER (order by `$col`)as ntil from $tableName) tt group by ntil ")
//      log.info("col is :" + col + ", min is :" + colMin.get(col) + ", max is : " + colMax.get(col))
//      // when the column  data contains null, the min and max may be null or be "Infinity".
//      if (colMin == null || colMin.get(col) == null || colMax.get(col) == null || colMax.get(col) == "Infinity" || colMin.get(col) == "Infinity") {
//        log.info("col is :" + col + ", min is :" + colMin.get(col) + ", max is : " + colMax.get(col))
//      } else {
//        //need toString first, then toDouble。 or：ClassCastException
//        val min = colMin.get(col).toString.toDouble
//        val max = colMax.get(col).toString.toDouble
//        val df2 = getHistogramMathDF(col, hiveContext, tableName, min, max, partNum)
//        dfArr.append(df1)
//        dfArr.append(df2)
//      }
//    }
//    //可能存在没有列可统计的情况， e.g. 表中的列都为double，但数据都是null.
//    //dfArr.reduce 和会报错：java.lang.UnsupportedOperationException: empty.reduceLeft
//    //总行数为0时，四分位，条形图也自然获取不到，且会出现NullPointerException。
//    if(dfArr.isEmpty || totalrows == 0L){
//      jsonobj.put("colUnique", colUnique)
//      jsonobj.put("colMissing", colMissing)
//    }else {
//      val dfAll = dfArr.reduce(_.unionAll(_))
//      val allRows = dfAll.collect()
//      val mathColMapQuartile = new java.util.HashMap[String, Array[java.util.HashMap[String, Long]]] //四分位
//      val mathColMapHistogram = new java.util.HashMap[String, Array[java.util.HashMap[String, Long]]] //条形图
//      val strColMapHistogram = new java.util.HashMap[String, Array[java.util.HashMap[String, Long]]] //条形图
//      val (mathColMapQuartile1, mathColMapHistogram1, strColMapHistogram1) = readRows(allRows)
//      for (col <- strColArr) {
//        strColMapHistogram.put(col, strColMapHistogram1.get(col).toArray[java.util.HashMap[String, Long]])
//      }
//      for (col <- mathColArr) {
//        mathColMapQuartile.put(col, mathColMapQuartile1.get(col).toArray[java.util.HashMap[String, Long]])
//        mathColMapHistogram.put(col, mathColMapHistogram1.get(col).toArray[java.util.HashMap[String, Long]])
//      }
//      jsonobj.put("mathColMapQuartile", mathColMapQuartile)
//      jsonobj.put("mathColMapHistogram", mathColMapHistogram)
//      jsonobj.put("strColMapHistogram", strColMapHistogram)
//      jsonobj.put("colUnique", colUnique)
//      jsonobj.put("colMissing", colMissing)
//    }
//    jsonobj
//  }
//  def readRows(rows: Array[Row]) : (java.util.HashMap[String, ArrayBuffer[java.util.HashMap[String,Long]]] , java.util.HashMap[String, ArrayBuffer[java.util.HashMap[String,Long]]], java.util.HashMap[String, ArrayBuffer[java.util.HashMap[String,Long]]])={
//    val mathColMapQuartile = new java.util.HashMap[String, ArrayBuffer[java.util.HashMap[String,Long]]] //四分位
//    val mathColMapHistogram = new java.util.HashMap[String, ArrayBuffer[java.util.HashMap[String,Long]]]//条形图
//    val strColMapHistogram = new java.util.HashMap[String, ArrayBuffer[java.util.HashMap[String,Long]]]//条形图
//    rows.foreach( row => {
//      val colName = row.getAs[String]("colName")
//      if (colName.startsWith("StrHistogram")) {
//        val value = row.getAs[String](1)
//        val num = row.getAs[Long](2)
//        val map = new java.util.HashMap[String, Long]()
//        val col = colName.substring(colName.indexOf('_') + 1)
//        map.put(value, num)
//        val mapValue = strColMapHistogram.get(col)
//        if (mapValue == null) {
//          val mapValueNew = ArrayBuffer[java.util.HashMap[String, Long]]()
//          mapValueNew.append(map)
//          strColMapHistogram.put(col, mapValueNew)
//        } else {
//          mapValue.append(map)
//          strColMapHistogram.put(col, mapValue)
//        }
//      } else if (colName.toString.startsWith("Quartile")) {
//        val value = row.get(1).toString
//        val num = row.getAs[Long](2)
//        val map = new java.util.HashMap[String, Long]()
//        val col = colName.substring(colName.indexOf('_') + 1)
//        map.put(value, num)
//        val mapValue = mathColMapQuartile.get(col)
//        if (mapValue == null) {
//          val mapValueNew = ArrayBuffer[java.util.HashMap[String, Long]]()
//          mapValueNew.append(map)
//          mathColMapQuartile.put(col, mapValueNew)
//        } else {
//          mapValue.append(map)
//          mathColMapQuartile.put(col, mapValue)
//        }
//      } else if (colName.toString.startsWith("MathHistogram")) {
//        val value =row.get(1).toString
//        val num = row.getAs[Long](2)
//        val map = new java.util.HashMap[String, Long]()
//        val col = colName.substring(colName.indexOf('_') + 1)
//        map.put(value, num)
//        val mapValue = mathColMapHistogram.get(col)
//        if (mapValue == null) {
//          val mapValueNew = ArrayBuffer[java.util.HashMap[String, Long]]()
//          mapValueNew.append(map)
//          mathColMapHistogram.put(col, mapValueNew)
//        } else {
//          mapValue.append(map)
//          mathColMapHistogram.put(col, mapValue)
//        }
//      }
//    })
//    (mathColMapQuartile, mathColMapHistogram, strColMapHistogram)
//  }
//  /** 数值型的列的条形分布获取方法*/
//  def getHistogramMathDF(col : String, hiveContext: HiveContext, tableName: String, min: Double, max: Double, partNum: Int) : DataFrame = {
//    val len = (max - min) / partNum
//    log.info(s"len is : $len")
//    val sb = new StringBuffer()
//    sb.append(s"select `$col`, (case ")
//    val firstRight = min + len
//    sb.append(s" when (`$col` >= $min and `$col` <= $firstRight) then 1 ")
//    for (i <- 2 until (partNum + 1)) {
//      val left = min + len * (i - 1)
//      val right = min + len * i
//      sb.append(s" when (`$col` > $left and `$col` <= $right) then $i ")
//    }
//    sb.append(s" else 0  end ) as partNum from $tableName")
//    sb.insert(0, s"select 'MathHistogram_$col' as colName, partNum, count(1) as num from ( ")
//    sb.append(") temptableScala group by partNum")
//    log.info("getHistogram is: " + sb.toString)
//    val df = hiveContext.sql(sb.toString)
//    df
//  }
//  def getDesfromDF(dfdesc : DataFrame, mathColArr: Array[String]):
//  (java.util.HashMap[String, Double], java.util.HashMap[String, Double], java.util.HashMap[String, Double], java.util.HashMap[String, Double], java.util.HashMap[String, Double])= {
//    val allRows = dfdesc.collect()
//    val colMin = new java.util.HashMap[String, Double]//最小值
//    val colMax = new java.util.HashMap[String, Double]//最大值
//    val colMean = new java.util.HashMap[String, Double]//平均值
//    val colStddev = new java.util.HashMap[String, Double]//标准差
//    val colMedian = new java.util.HashMap[String, Double]//中位值
//    allRows.foreach(row => {
//      val mapKey = row.getAs[String]("summary")
//      for(col <- mathColArr){
//        if("mean".equalsIgnoreCase(mapKey)){
//          colMean.put(col, row.getAs[Double](col))
//        }else if("stddev".equalsIgnoreCase(mapKey)){
//          colStddev.put(col, row.getAs[Double](col))
//        }else if("min".equalsIgnoreCase(mapKey)){
//          log.info("col is " + col +", min is : "+ row.getAs[Double](col))
//          colMin.put(col, row.getAs[Double](col))
//        }else if("max".equalsIgnoreCase(mapKey)){
//          log.info("col is " + col +", max is : "+ row.getAs[Double](col))
//          colMax.put(col, row.getAs[Double](col))
//        }else{
//          colMedian.put(col, row.getAs[Double](col))
//        }
//      }
//    })
//    (colMin, colMax, colMean, colStddev, colMedian)
//  }
//}
