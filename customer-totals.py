from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerTotals")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    dollarAmount = float(fields[2])
    return (customerID, dollarAmount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByID = rdd.reduceByKey(lambda x, y: x + y)
results = sorted(totalsByID.collect(), key=lambda x: x[1])

##### or could do the following ######
# flipped = totalsByID.map(lambda x: (x[1], x[0]))
# totalsByCustomerSorted = flipped.sortByKey()
# results = totalsByCustomerSorted.collect()



for result in results:
    print(result)
