#! /usr/bin/env python
# -*- coding: utf-8 -*- 

import codecs
import time
from collections import Counter
import CMUTweetTagger

from datetime import datetime
import fastcluster
from itertools import cycle
import json
import nltk
import numpy as np
import re

import os
import scipy.cluster.hierarchy as sch
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import preprocessing
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.metrics.pairwise import cosine_similarity
from sklearn import metrics

import string
import sys
import time
from datetime import datetime

def load_stopwords():
	stop_words = nltk.corpus.stopwords.words('english')
	stop_words.extend(['this','that','the','might','have','been','from',
                'but','they','will','has','having','had','how','went'
                'were','why','and','still','his','her','was','its','per','cent',
                'a','able','about','across','after','all','almost','also','am','among',
                'an','and','any','are','as','at','be','because','been','but','by','can',
                'cannot','could','dear','did','do','does','either','else','ever','every',
                'for','from','get','got','had','has','have','he','her','hers','him','his',
                'how','however','i','if','in','into','is','it','its','just','least','let',
                'like','likely','may','me','might','most','must','my','neither','nor',
                'not','of','off','often','on','only','or','other','our','own','rather','said',
                'say','says','she','should','since','so','some','than','that','the','their',
                'them','then','there','these','they','this','tis','to','too','twas','us',
                'wants','was','we','were','what','when','where','which','while','who',
                'whom','why','will','with','would','yet','you','your','ve','re','rt', 'retweet', '#fuckem', '#fuck',
                'fuck', 'ya', 'yall', 'yay', 'youre', 'youve', 'ass','factbox', 'com', '&lt', 'th',
                'retweeting', 'dick', 'fuckin', 'shit', 'via', 'fucking', 'shocker', 'wtf', 'hey', 'ooh', 'rt&amp', '&amp',
                '#retweet', 'retweet', 'goooooooooo', 'hellooo', 'gooo', 'fucks', 'fucka', 'bitch', 'wey', 'sooo', 'helloooooo', 'lol', 'smfh'])

	stop_words = set(stop_words)
	return stop_words


def normalize_text(text):
	try:
		text = text.encode('utf-8')
	except: pass
	text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))','', text)
	text = re.sub('@[^\s]+','', text)
	text = re.sub('#([^\s]+)', '', text)
	text = re.sub('[:;>?<=*+()/,\-#!$%\{˜|\}\[^_\\@\]1234567890’‘]',' ', text)
	text = re.sub('[\d]','', text)
	text = text.replace(".", '')
	text = text.replace("'", ' ')
	text = text.replace("\"", ' ')
	#text = text.replace("-", " ")
	#normalize some utf8 encoding
	text = text.replace("\x9d",' ').replace("\x8c",' ')
	text = text.replace("\xa0",' ')
	text = text.replace("\x9d\x92", ' ').replace("\x9a\xaa\xf0\x9f\x94\xb5", ' ').replace("\xf0\x9f\x91\x8d\x87\xba\xf0\x9f\x87\xb8", ' ').replace("\x9f",' ').replace("\x91\x8d",' ')
	text = text.replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8",' ').replace("\xf0",' ').replace('\xf0x9f','').replace("\x9f\x91\x8d",' ').replace("\x87\xba\x87\xb8",' ')
	text = text.replace("\xe2\x80\x94",' ').replace("\x9d\xa4",' ').replace("\x96\x91",' ').replace("\xe1\x91\xac\xc9\x8c\xce\x90\xc8\xbb\xef\xbb\x89\xd4\xbc\xef\xbb\x89\xc5\xa0\xc5\xa0\xc2\xb8",' ')
	text = text.replace("\xe2\x80\x99s", " ").replace("\xe2\x80\x98", ' ').replace("\xe2\x80\x99", ' ').replace("\xe2\x80\x9c", " ").replace("\xe2\x80\x9d", " ")
	text = text.replace("\xe2\x82\xac", " ").replace("\xc2\xa3", " ").replace("\xc2\xa0", " ").replace("\xc2\xab", " ").replace("\xf0\x9f\x94\xb4", " ").replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8\xf0\x9f", "")
	return text
		
def nltk_tokenize(text):
	tokens = []
	pos_tokens = []
	entities = []
	features = []

	try:
			tokens = text.split()

			for word in tokens:
				if word.lower() not in stop_words and len(word) > 1:
					features.append(word)
	except: pass
	return [tokens, pos_tokens, entities, features]

def process_json_tweet(text, fout, debug):
	features = []

	if len(text.strip()) == 0:
		return []
	text = normalize_text(text)

	try:
		[tokens, pos_tokens, entities, features] = nltk_tokenize(text)
	except: 
		print "nltk tokenize+pos pb!"
	return features

def custom_tokenize_text(text):
	REGEX = re.compile(r",\s*")
	tokens = []
	for tok in REGEX.split(text):
		if "@" not in tok:
			tokens.append(tok.strip().lower())

	return tokens


def spam_tweet(text):
	if 'Jordan Bahrain Morocco Syria Qatar Oman Iraq Egypt United States' in text:
		return True
		
	if 'Some of you on my facebook are asking if it\'s me' in text:
		return True
		
	if '@kylieminogue please Kylie Follow Me, please' in text:
		return True
	
	if 'follow me please' in text:
		return True
	
	if 'please follow me' in text:
		return True	
	
	if 'Want to win 12 Days of Christmas Giveaway Extravaganza? I just entered to win and you can too' in text:
		return True	
	
	if 'How to make Christmas Tree Brownies' in text:
		return True
	
	if 'Halloween for Christmas Pumpkin' in text:
		return True
	
	if 'Not-so-ugly Christmas sweaters and a' in text:
		return True
	
	if 'I added a video to a' in text:
		return True
	
	if 'On the first day of Christmas' in text:
		return True

	if '50 voucher for educational toys from @brightmindsuk via @plutoniumsox perfect for #Christmas!' in text:
		return True

	if '20 #Amazon gift cards with @MummyTries to help with my #Christmas shopping' in text:
		return True

	if "It's beginning to look a lot like #Christmas because we've got our cookie lineup!" in text:
		return True

	if '#Win a Fire 8GB Tablet with @AttachmentMumma - perfect for #Christmas' in text:
		return True

	if 'OUR #CHRISTMAS SALE IS NOW HERE!!! Get $3 off every #graphictee from now til Christmas Day' in text:
		return True

	if '#HandmadeJewelry' in text:
		return True

	if '50cm 240 LED Lights 8 Tubes Meteor Shower Rain Snowfall Tree Home Garden Xmas US' in text:
		return True

	if 'ASUS_ROGUK Christmas Swag Giveaway' in text:
		return True

	if '#OneHellOfANight' in text:
		return True

	if 'Teach your child to play piano with colors' in text:
		return True

	if 'hi @Harry_Styles Christmas is coming!!! I\'m very anxious and I hope you do too. Could you give me your Christmas gift following me?' in text:
		return True

	if '#BrokeThePot Ugly Christmas Sweaters Available Now on Our site' in text:
		return True

	if 'I want for Christmas, that you read my biography.' in text:
		return True

	if 'Enter to win in the 12 Days of Christmas Giveaways!' in text:
		return True

	if 'Christmas is coming, could you follow me? It would be a wonderful gift' in text:
		return True

	if 'Merry Christmas my angel I hope youre good.Please make my Christmas wish come true and Follow Me' in text:
		return True

	if 'My gift will be a Dildo. Discover which will be your #Christmas gift' in text:
		return True

	if 'Can you please make  my wish come true' in text:
		return True

	if 'Discover which will be your #Christmas gift' in text:
		return True

	if '@Harry_Styles' in text:
		return True

	if 'I liked a @YouTube' in text:
		return True

	if 'All I want is for you to follow me' in text:
		return True

	return False

if __name__ == "__main__":

	processedInputFile = codecs.open(sys.argv[1], 'r', 'utf-8')

	fout = codecs.open(sys.argv[2], 'w', 'utf-8')

	debug=0
	stop_words = load_stopwords()

	tid_to_raw_tweet = {}
	window_corpus = []
	tid_to_urls_window_corpus = {}
	tids_window_corpus = []
	dfVocTimeWindows = {}
	t = 0
	ntweets = 0
	tid_to_followers = {}

	startTime = time.time()

	for line in processedInputFile:
		ntweets += 1

		[tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends] = eval(line)
		if spam_tweet(text):
			continue

		features = process_json_tweet(text, fout, debug)
		tweet_bag = ""
		try:
			for user in set(users):
				tweet_bag += "@" + user.decode('utf-8').lower() + ","
			for tag in set(hashtags):
				if tag.decode('utf-8').lower() not in stop_words: 
					tweet_bag += "#" + tag.decode('utf-8').lower() + ","
			for feature in features:
				tweet_bag += feature.decode('utf-8') + ","
		except:
			pass

		if len(users) < 3 and len(hashtags) < 3 and len(features) > 3 and len(tweet_bag.split(",")) > 4 and not str(features).upper() == str(features):
			tweet_bag = tweet_bag[:-1]
			window_corpus.append(tweet_bag)
			tids_window_corpus.append(tweet_id)
			tid_to_urls_window_corpus[tweet_id] = media_urls
			tid_to_raw_tweet[tweet_id] = text
			tid_to_followers[tweet_id] = nfollowers


	#first only cluster tweets
	print "Raw tweet count: ",ntweets
	print "size of window_corpus: ",len(window_corpus)

	vectorizer = CountVectorizer(tokenizer=custom_tokenize_text, binary=True, min_df=max(int(len(window_corpus)*0.0025), 10), ngram_range=(2,3))
	docTermMatrix = vectorizer.fit_transform(window_corpus)

	print "size of docTermMatrix: ",docTermMatrix.shape
	map_index_after_cleaning = {}
	# Xclean = np.zeros((1, docTermMatrix.shape[1]))

	XcleanIndex = 0

	listOfSelectedDocs = []
	for i in range(0, docTermMatrix.shape[0]):
		#keep sample with size at least 5
		if docTermMatrix[i].sum() > 2:
			
			listOfSelectedDocs.append(docTermMatrix[i].toarray())
			# Xclean = np.vstack([Xclean, docTermMatrix[i].toarray()])
			map_index_after_cleaning[XcleanIndex] = i
			XcleanIndex+=1

	del docTermMatrix

	docTermMatrix = np.vstack(listOfSelectedDocs)
	# Xclean = Xclean[1:,]

	print "docTermMatrix after cleaning:", docTermMatrix.shape
	# print "NoOfTweets after restricting length:", Xclean.shape

	# X = Xclean
	Xdense = np.matrix(docTermMatrix).astype('float')
	X_scaled = preprocessing.scale(Xdense)
	X_normalized = preprocessing.normalize(X_scaled, norm='l2')

	vocX = vectorizer.get_feature_names()
	print "Vocabulary (tweets):", vocX

	boost_entity = []

	for term in vocX:
		pos_token = CMUTweetTagger.runtagger_parse([term.upper()])
		# print pos_token
		boostLevel = 1.0
		for each in pos_token[0]:
			# print "EACH \n",each
			if "^" in each[1]:
				# print "\nboost"
				boostLevel = 2.5*each[2]
				break
		boost_entity.append(boostLevel)


	dfX = docTermMatrix.sum(axis=0)

	dfVoc = {}
	keys = vocX
	vals = dfX
	for k,v in zip(keys, vals):
		dfVoc[k] = v


	distMatrix = pairwise_distances(X_normalized, metric='cosine')

	#cluster tweets
	print "fastcluster, average, cosine"
	L = fastcluster.linkage(distMatrix, method='average')


	indL = sch.fcluster(L, 10, 'maxclust')
	print "indL:", indL
	freqTwCl = Counter(indL)
	print "n_clusters:", len(freqTwCl)
	print(freqTwCl)

	npindL = np.array(indL)
	print "npindL",npindL

	tid_to_topic = {}
	cluster_to_tids = {}
	hID = 1
	clusterToHeadlineDBFile = codecs.open(sys.argv[3],'w','utf-8')

	for clfreq in freqTwCl.most_common(10):
		cl = clfreq[0]
		freq = clfreq[1]

		print "\n(cluster, freq):", clfreq
		clidx = (npindL == cl).nonzero()[0].tolist()
		print "clidx", clidx
		docTermMatrixOfCluster = docTermMatrix[clidx]
		print "size X[clidx]", docTermMatrixOfCluster.shape
		noOfDocsInCluster = docTermMatrixOfCluster.shape[0]
		cluster_centroid = (docTermMatrixOfCluster.sum(axis=0))/noOfDocsInCluster

		print "centroid_array:", cluster_centroid

		boosted_cluster_centroid = np.multiply(cluster_centroid,np.matrix(boost_entity))

		print "boosted_cluster_centroid", boosted_cluster_centroid
		distMatrix = np.matrix(cosine_similarity(docTermMatrixOfCluster,boosted_cluster_centroid))

		indexOfMinDoc = np.argmax(distMatrix)

		docId = clidx[indexOfMinDoc]

		docId = map_index_after_cleaning[docId]


		headline_tid = tids_window_corpus[docId]

		headline_text = tid_to_raw_tweet[headline_tid]
		headline_text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))','', headline_text)
		headline_text = headline_text.encode('utf8', 'replace').replace("\n", ' ').replace("\t", ' ')

		tids = []
		for i in clidx:
			idx = map_index_after_cleaning[i]
			tids.append(tids_window_corpus[idx])
			tid_to_topic[tids_window_corpus[idx]] = hID


		cluster_to_tids[cl] = tids
		print "clusterNumber", cl
		print "clusterToTids", tids

		clusterToHeadlineDBFile.write(str(hID) + "\t" + headline_text.decode('utf8', 'ignore') + "\n")
		fout.write("\n\n" +"NEW TOPIC" + "\n" +headline_text.decode('utf8', 'ignore') + "\n\n" + str(tids))
		hID += 1


	clusterToHeadlineDBFile.close()
	processedInputFile.close()
	fout.close()
	print time.time() - startTime


	sourceFile = codecs.open(sys.argv[4],'r','utf-8')
	targetFile = codecs.open(sys.argv[5],'w','utf-8')

	for line in sourceFile.readlines():
		python_dict = json.loads(line)

		if python_dict['id'] in tid_to_topic:
			python_dict['Topic'] = str(tid_to_topic[python_dict['id']])
		else:
			python_dict['Topic'] = "Assorted"

		tweet_date_raw = python_dict['created_at']
		d = datetime.strptime(tweet_date_raw,'%a %b %d %H:%M:%S +0000 %Y')
		python_dict['created_at'] = d.strftime('%Y-%m-%d')+"T"+d.strftime('%H:00:00')+"Z"

		jsonOut = json.dumps(python_dict)
		targetFile.write(jsonOut + u'\n')

	sourceFile.close()
	targetFile.close()
