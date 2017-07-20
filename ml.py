#!/bin/env python
# -*- coding: utf-8 -*-

""" 
* author Allen.L
* \last modified 2017-07-20 16:37:11
""" 
from pyspark import SparkContext
sc=SparkContext()

user_data = sc.textFile("file:///data/sparkml/ml-100k/u.user")
user_data.first()

user_fields = user_data.map(lambda line: line.split('|'))
num_users = user_fields.map(lambda fields: fields[0]).count() 

num_genders = user_fields.map(lambda fields : fields[2]).distinct().count() 

num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count() 
print "Users: %d, genders: %d, occupations: %d, ZIP codes: %d"%(num_users,num_genders,num_occupations,num_zipcodes)

rating_data = sc.textFile("file:///data/sparkml/ml-100k/u.data")
print rating_data.first()
num_ratings = rating_data.count()

print 'Ratings: %d'% num_ratings
"""
rating_data = rating_data.map(lambda line: line.split('\t'))
ratings = rating_data.map(lambda fields: int(fields[2]))
max_rating=ratings.reduce(lambda x,y:max(x,y)

min_rating=ratings.reduce(lambda x,y:min(x,y))
mean_rating = ratings.reduce(lambda x,y:x+y)/num_ratings
median_rating = np.median(ratings.collect())
ratings_per_user = num_ratings/num_users;
ratings_per_movie = num_ratings/ num_movies
print 'Min rating: %d' %min_rating
print 'max rating: %d' % max_rating
print 'Average rating: %2.2f' %mean_rating
print 'Median rating: %d '%median_rating
print 'Average # of ratings per user: %2.2f'%ratings_per_user
print 'Average # of ratings per movie: %2.2f' % ratings_per_movie
"""
