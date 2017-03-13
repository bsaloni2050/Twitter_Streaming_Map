import boto3
import json
from watson_developer_cloud import AlchemyLanguageV1
# from multiprocessing import Pool
from threading import Thread


class Worker(Thread):
	def __init__(self, message):
		Thread.__init__(self)
		self.message = message


	def run(self):
		try:
			message = self.message
			
			tweet = {'text': message.body, 'user': message.message_attributes.get('User').get('StringValue'),
			'time': message.message_attributes.get('Time').get('StringValue'),
			'longitude': message.message_attributes.get('Longitude').get('StringValue'),
			'latitude': message.message_attributes.get('Latitude').get('StringValue')}
			#'sentiment': message.message_attributes.get('sentiment').get('StringValue')}

	        # sentiment = TextBlob(tweet['text']).sentiment.polarity

	        # if sentiment >= 0.0:
	        #     tweet['sentiment'] = 'positive'
	        # else:
	        #     tweet['sentiment'] = 'negative'

			print (tweet)

			publishResponse = client.publish(TopicArn = topicArn, Message = json.dumps(tweet))

		except:
			pass

		finally:
			message.delete()

class WorkerPool(Thread):
	def __init__(self):
		Thread.__init__(self)

	def run(self):
		while True:
			for message in queue.receive_messages(MessageAttributeNames = ['Time', 'User', 'Longitude', 'Latitude',]):
				if message:
					worker = Worker(message)
					worker.start()

if __name__ == '__main__':

	# Get AWS SQS queue named tweet
	# sqs = boto3.resource('sqs')
	# queue = sqs.get_queue_by_name(QueueName = 'tweet') 

	# Get AWS SNS 
	client = boto3.client('sns')
	response = client.create_topic(Name = 'tweets')
	topicArn = response['TopicArn']

	# Subscribe
	subscribeResponse = client.subscribe(TopicArn = topicArn, Protocol = 'http', Endpoint = 'http://flask-env.pj5s5sxjmc.us-west-2.elasticbeanstalk.com/')


	pool = WorkerPool()
	pool.start()

