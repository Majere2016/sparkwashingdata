#!/bin/env python
# -*- coding: utf-8 -*-

""" 
* author Allen.L
* \last modified 2017-07-20 18:22:08
""" 
from pyspark import SparkContext

sc = SparkContext()

rawData = sc.textFile("file:///data/learndata/ml-100k/u.data")

print rawData.first()

rawRatings = rawData.map(lambda x: x.split('\t'))

rawRatings.take(5)

from pyspark.mllib.recommendation import Rating,ALS

ratings = rawRatings.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))

print ratings.take(5)

model = ALS.train(ratings,50,10,0.01)
userFeatures = model.userFeatures()

print userFeatures.take(2)

print model.userFeatures().count()
print model.productFeatures().count()

print len(userFeatures.first()[1])
predictRating = model.predict(789,123)
print predictRating

topKRecs = model.recommendProducts(789,10)
print '给用户userID怼见其喜欢的item:'
for rec in topKRecs:
	print rec

moviesForUser = ratings.groupBy(lambda x :x.user).mapValues(list).lookup(789)
print len(moviesForUser[0])
for i in sorted(moviesForUser[0],key=lambda x:x.ratings,reverse = True):
	print i.product
