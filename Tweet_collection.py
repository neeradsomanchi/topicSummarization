#import the necessary methods from tweepy library
# -*- coding: utf-8 -*-
#!/usr/bin/env python
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import io


#Variables that contains the user credentials to access Twitter API 
access_token = "place access token here"
access_token_secret = "place secret access token here"
consumer_key = "place consumer key here"
consumer_secret = "place secret consumer key here"
tweetLimitEn = 1000000000
tweetLimitEs = 0
tweetLimitTr = 0
tweetLimitKo = 0
numberOfLangs = 1

class StdOutListener(StreamListener):
    
	def on_data(self, data):

		if self.tweetCountEn > tweetLimitEn:
			print "English Done! Total Tweets: " , self.tweetCountEn
			#self.tweetCountEn = 0
			return False
		elif self.tweetCountEs > tweetLimitEs:
			print "Spanish Done! Total Tweets: " , self.tweetCountEs
			#self.tweetCountEs = 0
			return False
		elif self.tweetCountTr > tweetLimitTr:
			print "Turkish Done! Total Tweets: " , self.tweetCountTr
			#self.tweetCountEs = 0
			return False
		elif self.tweetCountKo > tweetLimitKo:
			print "Korean Done! Total Tweets: " , self.tweetCountKo
			#self.tweetCountEs = 0
			return False

		if 'retweeted_status' in data:
			pass

		elif 'RT @' in data:
			pass

		elif 'quoted_status_id' in data:
			pass

		elif '"in_reply_to_status_id":null' in data:
			tweet = json.loads(data)
			if (tweet['lang'] == "en"):
				with io.open('Christmas_En_30th.json', 'a', encoding='utf-8') as file:
					file.write(data)
				self.tweetCountEn += 1
				print "English = " , self.tweetCountEn
			elif (tweet['lang'] == "es"):
				with io.open('USelections_es_5k_9th.json', 'a', encoding='utf-8') as file:
					file.write(data)
				self.tweetCountEs += 1
				print "Spanish = " , self.tweetCountEs
				self.tweetCountEs += 1
			elif (tweet['lang'] == "tr"):
				with io.open('USelections_tr_10k_9th.json', 'a', encoding='utf-8') as file:
					file.write(data)
				self.tweetCountTr += 1
				print "Turkish = " , self.tweetCountTr
			elif (tweet['lang'] == "ko"):
				with io.open('iPhone_ko_10k_9th.json', 'a', encoding='utf-8') as file:
					file.write(data)
				self.tweetCountKo += 1
				print "Korean = " , self.tweetCountKo

		return True

	def on_error(self, status):
		print status

	def on_reset_En (self):
		self.tweetCountEn = 0
		print "RESET Eng!!"

	def on_reset_Tr (self):
		self.tweetCountTr = 0
		print "RESET Turkish!!"

	def on_reset_Ko (self):
		self.tweetCountKo = 0
		print "RESET Korean!!"

	def tweetCountAccessEn (self):
		return self.tweetCountEn

	def tweetCountAccessEs (self):
		return self.tweetCountEs

	def tweetCountAccessTr (self):
		return self.tweetCountTr

	def tweetCountAccessKo (self):
		return self.tweetCountKo


if __name__ == '__main__':

	#This handles Twitter authetification and the connection to Twitter Streaming API
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, l)

	stream.filter(languages=["en"],track=["#christmas","christmas"])
	#This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
	# if numberOfLangs == 2:
	# 	if l.tweetCountAccessEn > tweetLimitEn:
	# 		l.on_reset_En()
	# 		stream.filter(languages=["es"],track=['#TrumpPence16','#election2016','#presidential','realDonaldTrump', 'HillaryClinton', '#HackingHillary', '#TrumpCantSwim', '#HillarysEarPiece'])
	# 	elif l.tweetCountAccessEs > tweetLimitEs:
	# 		l.on_reset_Es()
	# 		stream.filter(languages=["en"],track=['#TrumpPence16','#election2016','#presidential','realDonaldTrump', 'HillaryClinton', '#HackingHillary', '#TrumpCantSwim', '#HillarysEarPiece'])
	# elif numberOfLangs == 1:
	# 	stream.filter(languages=["ko"],track=['#AirPods','#AppleEvent', '#iPhone7', '#iPhone7launch', '@Apple', u'#애플이벤트', u'#에어팟', u'에어팟', u'애플이벤트', u'#아이폰7', '#AppleWatch'])
