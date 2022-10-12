# Databricks notebook source
fileroot = "clinicaltrial_2020"

import os
os.environ['fileroot'] = fileroot

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/clinicaltrial_2020_csv-2.gz","file:/tmp/clinicaltrial_2020_csv.gz")

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/clinicaltrial_2021_csv-2.gz","file:/tmp/clinicaltrial_2021_csv.gz")

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/clinicaltrial_2019__csv-1.gz","file:/tmp/clinicaltrial_2019_csv.gz")

# COMMAND ----------

# MAGIC %sh 
# MAGIC gunzip -d /tmp/ /tmp/clinicaltrial_2020_csv.gz
# MAGIC gunzip -d /tmp/ /tmp/clinicaltrial_2021_csv.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip -d /tmp/ /tmp/clinicaltrial_2019_csv.gz

# COMMAND ----------

ls /tmp

# COMMAND ----------

dbutils.fs.mv("file:/tmp/clinicaltrial_2020_csv", "/FileStore/tables/clinicaltrial_2020.csv", True )

# COMMAND ----------

dbutils.fs.mv("file:/tmp/clinicaltrial_2021_csv", "/FileStore/tables/clinicaltrial_2021.csv", True )

# COMMAND ----------

dbutils.fs.mv("file:/tmp/clinicaltrial_2019_csv", "/FileStore/tables/clinicaltrial_2019.csv", True )

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/clinicaltrial_2021_csv")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/clinicaltrial_2020_csv")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/clinicaltrial_2019_csv")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/clinicaltrial_2021_csv")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/clinicaltrial_2020_csv")

# COMMAND ----------

# dbutils.fs.head("/FileStore/tables/"+fileroot+".csv")

# COMMAND ----------

myrdd = sc.textFile("dbfs:/FileStore/tables/"+fileroot+".csv")

# COMMAND ----------

header = myrdd.first()
myrdd_count = myrdd.filter(lambda row:row!=header)
myrdd_count.count()

# COMMAND ----------

myrdd = sc.textFile("dbfs:/FileStore/tables/"+fileroot+".csv")
splitrdd = myrdd.map(lambda x:x.split('|'))
splitrdd.take(2)

# COMMAND ----------

header = myrdd.first()
myrdd_count = myrdd.filter(lambda row:row!=header)
myrdd_count.count()

# COMMAND ----------

splitrdd = myrdd.map(lambda x:x.split('|'))

# COMMAND ----------

TypeRdd= splitrdd.map(lambda x: x[5])
TypeRdd.map(lambda k:(k,1)).reduceByKey(lambda j1,j2:j1+j2).sortBy(lambda j:j[1],ascending =False).take(4)

# COMMAND ----------



# COMMAND ----------

"""counting = myrdd.map(lambda clicnical: clicnical.split('|'))
header =counting.first()
con = counting.filter(lambda row:  row != header)
recounting = counting.flatMap(lambda clicnical: clicnical[7].split(',')).map(lambda x:[x,1]).reduceByKey(lambda v1,v2:v1+v2).map(lambda v:(v[1],v[0])).sortByKey(ascending = False).map(lambda v:(v[1],v[0])).filter(lambda c:(c[0]!=''))
recounting.take(5)"""

# COMMAND ----------

# DBTITLE 1,2021
salford = myrdd.map(lambda sal: sal.split('|'))
header =salford.first()
uni = salford.filter(lambda row:  row != header)
Salford1 = salford.flatMap(lambda sal: sal[7].split(',')).map(lambda s:[s,1]).reduceByKey(lambda y1,y2:y1+y2).map(lambda k:(k[1],k[0])).sortByKey(ascending = False).map(lambda k:(k[1],k[0])).filter(lambda l:(l[0]!=''))
Salford1.take(5)

# COMMAND ----------

# DBTITLE 1,Question Number 4
RDDmesh =  sc.textFile("dbfs:/FileStore/tables/mesh.csv")
#dbutils.fs.head("dbfs:/FileStore/tables/mesh.csv")

# COMMAND ----------

RDDmesh_Split=RDDmesh.map(lambda r: r.replace('"','').replace('.',',').split(','))\
.map(lambda l: (l[0],l[1]))
RDDmesh_Split.take(5)

# COMMAND ----------

header = RDDmesh_Split.first()
myrdd_count1 = RDDmesh_Split.filter(lambda row:row!=header)
myrdd_count1.count()

# COMMAND ----------

RDDmesh_Header = RDDmesh.first()
RDDmesh_Header_Remove = RDDmesh.filter(lambda Header: Header!=RDDmesh_Header)
RDDmesh_Split = RDDmesh_Header_Remove.map(lambda x:(x.split(",")[0], x.split(",")[1].split(".")[0]))
RDDmesh_Split.take(5)

# COMMAND ----------

Split.take(10)

# COMMAND ----------

counting2 = myrdd.map(lambda clicnical: clicnical.split('|'))
header =counting2.first()
con = counting2.filter(lambda row:  row != header)
recounting2 = counting2.map(lambda clicnical: clicnical[7]).flatMap(lambda x : x.split(",")).filter(lambda x: x != "").map(lambda x:[x,1])
recounting2.take(100)

# COMMAND ----------

""""myrdd_count1.take(20)"""

# COMMAND ----------

""""newRdd = recounting2.join(myrdd_count1)
newRdd.take(20)"""

# COMMAND ----------

newRdd1 = recounting2.join(RDDmesh_Split).map(lambda y: (y[1][1], 1)) \
    .reduceByKey(lambda v1, v2: v1+ v2).sortBy(lambda x: -x[1])
    
newRdd1.take(5)

# COMMAND ----------

counting3 = myrdd.map(lambda clicnical: clicnical.split('|'))
header =counting3.first()
con = counting3.filter(lambda row:  row != header)
recounting3 = counting3.map(lambda clicnical: clicnical[7]).flatMap(lambda x : x.split(",")).filter(lambda x: x != "").map(lambda x:[x,1])
recounting3.take(100)

# COMMAND ----------

 newRdd2 = recounting3.join(RDDmesh_Split).map(lambda y: (y[1][1], 1)) \
    .reduceByKey(lambda v1, v2: v1+ v2).sortBy(lambda x: -x[1])
    
newRdd2.take(5)

# COMMAND ----------

salford4 = myrdd.map(lambda sal: sal.split('|'))
header =salford4.first()
salford5 = salford4.filter(lambda row:  row != header)
NewSalford = salford4.map(lambda cl: cl[7]).flatMap(lambda z : z.split(",")).filter(lambda z: z != "").map(lambda z:[z,1])
NewSalford.take(100)

# COMMAND ----------

NEWRDD3 = NewSalford.join(RDDmesh_Split).map(lambda y: (y[1][1], 1)) \
    .reduceByKey(lambda v1, v2: v1+ v2).sortBy(lambda x: -x[1])
    
NEWRDD3.take(5)

# COMMAND ----------

NEWRDD3 = NewSalford.join(RDDmesh_Split).map(lambda Z: (Z[1][1], 1)) \
    .reduceByKey(lambda F1, F2: F1+ F2).sortBy(lambda Y: -Y[1])
    
NEWRDD3.take(5)

# COMMAND ----------

# DBTITLE 1,Q5
splitrdd = myrdd.map(lambda z:z.split('|'))
sep1 = splitrdd.map(lambda l:l[1])
sep1.take(2)

# COMMAND ----------

pharmardd = sc.textFile("/FileStore/tables/pharma.csv")
newpharma=pharmardd.map(lambda z: z.replace('"','').split(','))
newpharma.take(2)

# COMMAND ----------

pharmacomp=newpharma.map(lambda l:l[1])
pharmacomp.take(2)

# COMMAND ----------

"""sponsors = myrdd2021.map(lambda clicnical: clicnical.split('|'))
header = sponsors.first() #extract header
sponsors = sponsors.filter(lambda row:  row != header)
sponsorship = sponsors.map(lambda s: s[1])\
.map(lambda x: (x, 1))"""

# COMMAND ----------

sponsors = myrdd.map(lambda cl: cl.split('|'))
header = sponsors.first() #extract header
sponsors = sponsors.filter(lambda row:  row != header)
sponsorship = sponsors.map(lambda x: x[1])\
.map(lambda y: (y, 1))

# COMMAND ----------

pharmaRDD = sc.textFile('dbfs:/FileStore/tables/pharma.csv')
pharmaRDDheader = pharmaRDD.first() #extract header
pharmaRDD = pharmaRDD.filter(lambda row:  row != pharmaRDDheader) 
pharmacetical = pharmaRDD.map(lambda d: d.split(','))\
.map(lambda c: (c[1].replace('"', '')))\
.map(lambda x: (x,1))

# COMMAND ----------

pharmajoined = sponsorship.leftOuterJoin(pharmacetical).filter(lambda j : j[1][1] == None)\
.map(lambda z: (z[0], 1))\
.reduceByKey(lambda k1, k2: k1+ k2).sortBy(lambda x: -x[1])
pharmajoined.take(10)

# COMMAND ----------

# DBTITLE 1,Question Number 6 
final = myrdd.map(lambda clinical: clinical.split('|'))
header = final.first() #extract header
sponsors = final.filter(lambda row:  row != header)

# COMMAND ----------

completed = final.filter(lambda g:g[2]=='Completed').map(lambda q:q[4].split(' ')).filter(lambda l:l[0]!='').filter(lambda j:j[1]=='2021').map(lambda f:(f[0],1)).reduceByKey(lambda k1,k2:k1+k2).sortBy(lambda z:z[0])
completed.collect()

# COMMAND ----------

import calendar
x = {a:z for z,a in enumerate(calendar.month_abbr[1:],1)}

NewRDD=completed.sortBy(lambda a: x.get(a[0]))
NewRDD.collect()
