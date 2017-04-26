# Important the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream 
from elasticsearch import Elasticsearch
import requests
import json
import boto3


# Variables that contains the user credentials to access Twitter API
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


# This is a basic listener that just prints received tweets to stdout
class StdOutListener(StreamListener):

	def on_error(self, status):
		print (status)

	def on_status(self, status):
		#print(status)
		try:
			if status.coordinates:
				#print (status)
				tweet = {}
				tweet['user'] = status.user.screen_name
				tweet['text'] = status.text
				tweet['location'] = status.coordinates['coordinates']
				tweet['time'] = str(status.created_at)
				print(tweet)

				# Store twitter data into elasticsearch
				es.index(index = 'twitter', doc_type = 'tweet', body = {
					'user': tweet['user'],
					'text': tweet['text'],
					'location': tweet['location'],
					'time': tweet['time'],
					})

				print ("EXECUTED")
				postURL = 'http://search-tweetmap-hozfp5wv6wvf7ajfcenijhcmmu.us-west-2.es.amazonaws.com/twitter/tweet'
				r = requests.post(postURL, json = tweet)

				Send message using AWS SQS
				response = queue.send_message(MessageBody = tweet['text'], MessageAttributes = {
					'Time': {
						'StringValue': tweet['time'],
						'DataType': 'String'
					},
					'User': {
						'StringValue': tweet['user'],
						'DataType': 'String'
					},
					'Longitude': {
						'StringValue': str(tweet['location'][0]),
						'DataType': 'String'
					},
					'Latitude': {
						'StringValue': str(tweet['location'][1]),
						'DataType': 'String'
					}
				})

				print (response.get('MessageId'))
				print (tweet)
		except Exception as e:
			print ('Error! {0}: {1}'.format(type(e), str(e)))	

if __name__ == '__main__':
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

	sqs = boto3.resource('sqs')
	queue = sqs.get_queue_by_name(QueueName = 'tweet')

	# Twitter authetification 
	print("after main")
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	print ("br strem")
	stream = Stream(auth, l)

	print ("after strem")

	stream.filter(track = ['gigi hadid', 'tommy', 'parada', 'google', 'trump',
                             'new york', 'india', 'puten'])
	print ("after filter")
