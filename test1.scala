val ds=spark.range(5,100,5) //5到100，间隔5
ds.describe().show() 


import org.apache.spark.sql.types._
val sch=StructType(
StructField("PassengerId",IntegerType,true)::
StructField("Survived",IntegerType,true)::
StructField("Pclass",StringType,true)::
StructField("Name",StringType,true)::
StructField("Sex",StringType,true)::
StructField("Age",IntegerType,true)::
StructField("SibSp",IntegerType,true)::
StructField("Parch",IntegerType,true)::
StructField("Ticket",StringType,true)::
StructField("Fare",FloatType,true)::
StructField("Cabin",StringType,true)::
StructField("Embarked",StringType,true)::Nil
)
val train=spark.read.schema(sch).csv("/Hadoop/Input/train.csv")



import org.apache.spark.sql._
val train=spark.read.options(Map(("header","true"))).csv("/Hadoop/Input/train.csv")//注意这里Map的括号数量。

train.filter("Pclass is null").count()
train.filter("Name is null").count()
train.filter("Sex is null").count()
train.filter("Age is null").count()//177
train.filter("SibSp is null").count()
train.filter("Parch is null").count()
train.filter("Ticket is null").count()
train.filter("Fare is null").count()
train.filter("Cabin is null").count()//687
train.filter("Embarked is null").count()//2


val train1=train.na.fill(Map("Age"->train.select("Age").describe().collectAsList().get(1).get(1),"Embarked"->"S"))
//只能新建。
train1.filter("Age is null").count()//0


train.select("Age").describe().show()//选中某列

train.select("Age").describe().collectAsList().get(1).get(1)//取出平均值

train.groupBy("Embarked").count().show()//unique计数。

train.columns//返回string类型的数组

train.filter(train("Age")>50).show()//filter是筛选的意思。

//
尝试机器学习前，先看这个数据如何分析
drop name这一列
cabin，第一，多字段的，记录长度。第二，给出英文字符，只取有数字跟在后面的那种。null项取Z。
只留下这两项。drop cabin
ticket 纯数字 英文+纯数字 带.的英文+纯数字 带-的英文+纯数字 带.带-的英文+纯数字
完全无法解读，直接删了。
先让test["survive"]=-1 然后合并train和test，然后记录index。
然后搞特征，调包。





val df = Seq(("Ram",null,"MCA","Bangalore"),(null,"25",null,null),(null,"26","BE",null),("Raju","21","Btech","Chennai")).toDF("name","age","degree","Place")
df.show(false)
val df2 = df.columns.foldLeft(df)( (df,c) => df.withColumn(c+"_null", when(col(c).isNull,1).otherwise(0) ) )
df2.createOrReplaceTempView("student")
val sql_str_null = df.columns.map( x => x+"_null").mkString(" ","+"," as null_count ")
val sql_str_full = df.columns.mkString( "select ", ",", " , " + sql_str_null + " from student")
spark.sql(sql_str_full).show(false)