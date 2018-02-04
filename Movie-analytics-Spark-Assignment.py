
# coding: utf-8

# ### useful online doc: ###
# - Spark DataFrames:  [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# - [Spark DataFrames API doc](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)
# - [Another third party Spark tutorial](http://www.sparktutorials.net/)

# ### main source for the datasets used in this notebook: ###
# [the movie dataset](https://www.kaggle.com/rounakbanik/the-movies-dataset) on Kaggle

# ### the relevant movies-metadata.csv file is loaded for you into a DF in the code below

# In[34]:


from pyspark.sql import SparkSession
import ast
import matplotlib.pyplot as plt
import numpy as np

SPARK_HOME="/usr/local/spark/"
WD = "."
RESOURCES = WD+"/../../resources/"


# In[35]:


## SparkSession is one main connection with the Spark driver
session = SparkSession     .builder     .appName("Movie analytics in Spark v.1")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# We read from csv file using the session we have just creeated

# In[36]:


mmDF = session.read.csv(path = RESOURCES+"/movies_metadata.csv", inferSchema=True, header=True, mode='DROPMALFORMED', multiLine=True)


# In[30]:


mmDF.printSchema()


# `mmDF` is an object of class:
# `pyspark.sql.DataFrame`
# 
# let us look at the first row: this is a "raw" view of the data

# In[49]:


mmDF.head()


# In[199]:


### task 1: produce a DF that only contains the movie title and budget, display the first 10 rows
movies = mmDF[["title", "budget"]]
movies.show(10)


# In[200]:


### task2: show 10 rows from movies with a budget less than 5000000
tenMoviesBudget = movies.where(movies.budget < 5000000).select("title", "budget")
tenMoviesBudget.show(10)


# In[201]:


### task3: show the 10 most expensive movies under 5000000
#newfloatdf is the new Dateframe transformed to use float instead of a string, using the dataframe from above to save computation
newfloatdf = convertColumn(tenMoviesBudget, "budget", "float")
highestMoviesBelow = newfloatdf.orderBy("budget",ascending=False)
highestMoviesBelow.show(10)
    
    


# In[202]:


def convertColumn(df, name, new_type):
    df_1 = df.withColumnRenamed(name, "swap")
    return df_1.withColumn(name, df_1['swap'].cast(new_type)).drop("swap")


# In[203]:


allGenresFromFileDF = session.read.parquet(RESOURCES+"/genres.parquet")
allGenresFromFileDF.take(10)

## task 3: produce a count of the number of occurrences of each genre.

genresDF = allGenresFromFileDF.drop("id")
orderedGenres = genresDF.groupBy("name").count().orderBy("count", ascending=False)
orderedGenres.show(10)


# ## task 4: plot the sorted genres on a barchart
# 
# hint: you need to extract the data (names) and xlabels (count) from yout orderedGenre df. Each of them should be a list of values.
# 
# Call these lists `data` and `xlabels`, respectively. 
# 
# The code for `xlabels` is given below. Please create teh xlables similarly. then reuse code from the 
# 
# `EDA-housing-Spark` 
# 
# notebook shown in class to produce the plot.

# In[204]:


## this is how you create the data list:

## note this code won't work unless you have correctly defined your orderedGenres DF!
xlabels = [ row['name'] for row in orderedGenres.select('name').collect() ] 

ylabels =  [ row['count'] for row in orderedGenres.select('count').collect() ] 

fig, genres = plt.subplots()
ind = np.arange(len(ylabels))
width = 0.8

genres.bar(ind, ylabels)
genres.set_title('orderedGenres')
genres.set_ylabel('count')
genres.set_xlabel('Genres')
genres.set_xticks(ind + width / 2)
genres.set_xticklabels(xlabels, rotation='vertical')
plt.show()


# ## task 5  (bonus task): group the movies by genre and find the average budget for each genre
# 
# you are going to use the pre-defined `genre_movie` DF, which we load for you here.
# 
# **hint**: you will need to join the genre_movie DF with the original mmDF movies_metadata DF
# 
# documentation for  [`DataFrame.join` operator](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame) 

# In[205]:


genre_movie = spark.read.parquet(RESOURCES+"/genres_movies.parquet")


# In[206]:


genre_movie.printSchema()
newfloatdf = convertColumn(mmDF, 'budget', 'float')
queryDFnewDF = genre_movie.join(newfloatdf, newfloatdf.id == genre_movie.id).groupBy("genre").avg("budget").orderBy('avg(budget)', ascending=False).select("genre" , "avg(budget)")
queryDFnewDF.take(10)

