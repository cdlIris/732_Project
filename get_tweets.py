import tweepy
import sys
import jsonpickle
import os, csv
API_KEY = '4QYQl1GOAqOPBKpmCsWWtRe3D'
API_SECRET = 'OCAyD7rtNVlaP8kDHx17rkp2SN1UElpum1pCx66BsSvwwPUOzb '
# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = tweepy.AppAuthHandler("PQEim5Uq9jFq3YiMGF12CS7oz", "8gYnr83KbscFqaqE0I5vvGKIjehcVXwGvd43fvR7UL2iEpzhyE")
# auth.set_access_token("1065559266784878592-9VP0iOYDmVzkD84iaEKNVZHk0jb6fi",
                        #   "9qETNhtRPrN02QpG4yyTqZnj101HqYPQXViVO5veWm964")

api = tweepy.API(auth, wait_on_rate_limit=True,
				   wait_on_rate_limit_notify=True)

# https://bhaskarvk.github.io/2015/01/how-to-use-twitters-search-rest-api-most-effectively./
if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)


searchQuery = '#bitcoin -filter:retweets'  # this is what we're searching for
maxTweets = 10000000 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits
fName = 'tweets.txt' # We'll store the tweets in a text file.


# If results from a specific ID onwards are reqd, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
sinceId = None

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = -1

tweetCount = 0
until = '2019-11-17'
print("Downloading max {0} tweets".format(maxTweets))
csvFile = open('tweets.csv', 'a')
csvWriter = csv.writer(csvFile)
while tweetCount < maxTweets:
    try:
        if (max_id <= 0):
            if (not sinceId):
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry, until=until)
            else:
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                            since_id=sinceId, until=until)
        else:
            if (not sinceId):
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                            max_id=str(max_id - 1), until=until)
            else:
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                            max_id=str(max_id - 1),
                                            since_id=sinceId, until=until)
        if not new_tweets:
            print("No more tweets found")
            break
        for tweet in new_tweets:
            csvWriter.writerow([tweet.created_at, tweet.text.encode('utf-8')])
        tweetCount += len(new_tweets)
        print("Downloaded {0} tweets".format(tweetCount))
        max_id = new_tweets[-1].id
    except tweepy.TweepError as e:
        # Just exit if any error
        print("some error : " + str(e))
        break
csvFile.close()
print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))