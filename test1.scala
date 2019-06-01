val ds=spark.range(5,100,5) //5��100�����5
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
val train=spark.read.options(Map(("header","true"))).csv("/Hadoop/Input/train.csv")//ע������Map������������

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
//ֻ���½���
train1.filter("Age is null").count()//0


train.select("Age").describe().show()//ѡ��ĳ��

train.select("Age").describe().collectAsList().get(1).get(1)//ȡ��ƽ��ֵ

train.groupBy("Embarked").count().show()//unique������

train.columns//����string���͵�����

train.filter(train("Age")>50).show()//filter��ɸѡ����˼��

//
���Ի���ѧϰǰ���ȿ����������η���
drop name��һ��
cabin����һ�����ֶεģ���¼���ȡ��ڶ�������Ӣ���ַ���ֻȡ�����ָ��ں�������֡�null��ȡZ��
ֻ���������drop cabin
ticket ������ Ӣ��+������ ��.��Ӣ��+������ ��-��Ӣ��+������ ��.��-��Ӣ��+������
��ȫ�޷������ֱ��ɾ�ˡ�
����test["survive"]=-1 Ȼ��ϲ�train��test��Ȼ���¼index��
Ȼ���������������





val df = Seq(("Ram",null,"MCA","Bangalore"),(null,"25",null,null),(null,"26","BE",null),("Raju","21","Btech","Chennai")).toDF("name","age","degree","Place")
df.show(false)
val df2 = df.columns.foldLeft(df)( (df,c) => df.withColumn(c+"_null", when(col(c).isNull,1).otherwise(0) ) )
df2.createOrReplaceTempView("student")
val sql_str_null = df.columns.map( x => x+"_null").mkString(" ","+"," as null_count ")
val sql_str_full = df.columns.mkString( "select ", ",", " , " + sql_str_null + " from student")
spark.sql(sql_str_full).show(false)