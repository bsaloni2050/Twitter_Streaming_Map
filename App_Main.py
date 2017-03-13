from flask import Flask, render_template, request
from flask_socketio import SocketIO, send, emit
import json
import requests

application = Flask(__name__)

socketio = SocketIO(application)

socketConnected = False


@application.route('/', methods=['GET', 'POST'])
def hello_world():
    global socketConnected

    if request.method == 'POST':

        try:
            js = json.loads(request.data)
        except:
            pass

        hdr = request.headers.get('X-Amz-Sns-Message-Type')

        if hdr == 'SubscriptionConfirmation' and 'SubscribeURL' in js:
            r = requests.get(js['SubscribeURL'])

        if hdr == 'Notification':
            tweet = js['Message']
            print(tweet)
            postURL = 'http://localhost:9200/twitter/tweet/tweet'
            r = requests.post(postURL, json = tweet)

            if socketConnected:
                socketio.emit('realTimeResponse', tweet)

    return render_template('TwitterMap.html')


@application.route('/search')
def handle_search():
    return render_template('search.html')


@socketio.on('realTime')
def handle_realtime_event(message):
    global socketConnected
    socketConnected = True

    queryURL = 'http://localhost:9200/twitter/tweet/_search?q=*:*&size=1000'
    response = requests.get(queryURL)
    results = json.loads(response.text)
    print (results)

    tweets = []
    for result in results['hits']['hits']:
        tweet = { 'longitude': result['_source']['longitude'],'latitude': result['_source']['latitude']}
        tweets.append(tweet)

    send(json.dumps(tweets))

@socketio.on('message')
def handle_message(message):
    if message == 'Init':
        # Run local elastic search
        # Sending a request with the query keyword and loading the response tweets in results.
        queryURL = 'http://localhost:9200/twitter/tweet/_search?q=*:*&size=1000'
        response = requests.get(queryURL)
        results = json.loads(response.text)

        print("INIT MAP")
    else:

        queryKeyWord = message.replace(' ', '%20')
        # Sending a request with the querry keyword  and laoding the response tweets in results.
        queryURL = 'http://localhost:9200/twitter/tweet/_search?q=' + queryKeyWord + '&size=1000'
        response = requests.get(queryURL)
        results = json.loads(response.text)
        print("SEARCH" + str(message))

    tweets = []
    for result in results['hits']['hits']:
        tweet = { 'longitude': result['_source']['longitude'],'latitude': result['_source']['latitude']}
        tweets.append(tweet)

    send(json.dumps(tweets))


if __name__ == '__main__':
    socketio.run(application, host='0.0.0.0')
