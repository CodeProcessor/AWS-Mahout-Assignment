'''
Created on 3/1/20

@author: dulanj
'''


from klein import run, route
import redis
import os

# Start up a Redis instance
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Pull out all the recommendations from HDFS
p = os.popen("hadoop fs -cat recommendations/part*")

# Load the recommendations into Redis
for i in p:
    k, v = i.split('\t')
    r.set(k, v)


# Establish an endpoint that takes in user id in the path
@route('/<string:id>')
def recs(request, _id):
    # Get recommendations for this user
    _v = r.get(_id)
    return 'The recommendations for user '+_id+' are '+_v


# Make a default endpoint
@route('/')
def home(request):
    return 'Please add a user id to the URL, e.g. http://localhost:8080/1234n'


# Start up a listener on port 8080
run("localhost", 8080)
