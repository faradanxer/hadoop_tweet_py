import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
file_path = 'hdfs:///user/hapbox/tweets/input/ira_tweets_csv_hashed.csv'
tweetList = sc.textFile(file_path).map(lambda line: line[1:-1].split('","'))
tweet1 = tweetList.map(lambda row: (row[1], row[10], row[12]))
tweet2 = tweet1.filter(lambda row: row[1] == 'ru')
filter = tweet2.filter(lambda x: any(e in x[2] for e in [
    'Путин', 'Медведев','Шойгу','Лавров','Захарова','Песков',
'Володин','Яровая','Жириновский','Валуев','Голикова','Собянин','Милонов','Миронов',
]))
userList = filter.map(lambda row: row[0]).map(lambda x: (x, 1))
countedUserList = userList.reduceByKey(lambda row, n: row + n)
result = countedUserList.sortBy(lambda row: row[1], ascending=False)
print(result.take(1))