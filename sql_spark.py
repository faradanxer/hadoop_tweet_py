import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)
df = spark.read.csv("hdfs:///user/hapbox/tweets/input/ira_tweets_csv_hashed.csv", inferSchema=True, header=True)
df.createGlobalTempView("tweets")
spark.sql("SELECT userid, COUNT(userid) as count_tweets FROM global_temp.tweets WHERE account_language = 'ru' AND (tweet_text LIKE '%Путин%' OR tweet_text LIKE '%Медведев%' OR tweet_text LIKE '%Шойгу%' OR tweet_text LIKE '%Лавров%' OR tweet_text LIKE '%Захарова%' OR tweet_text LIKE '%Песков%' OR tweet_text LIKE '%Володин%' OR tweet_text LIKE '%Яровая%' OR tweet_text LIKE '%Жириновский%' OR tweet_text LIKE '%Валуев%' OR tweet_text LIKE '%Голикова%' OR tweet_text LIKE '%Собянин%' OR tweet_text LIKE '%Милонов%' OR tweet_text LIKE '%Миронов%') GROUP BY userid ORDER BY count_tweets DESC LIMIT 1").show()
