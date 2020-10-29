## Speech Trends on Social Media
I developed this project during my Data Engineering Fellowship at Insight Data Science that can help social network's operational teams to perform comprehensive content moderation and content filtering.
## Motivation:
* Create a Web app to detect hate speech trends on social media.
* Identification of hate content is an important challenge for the social networks and many posts on the social networks may have hate speech or toxic content. 
* The Engineering motivation behid this web app was to create an efficiant, robust and scalable big data batching pipeline using opensource and NoSQL technology.
## Data Pipeline
![alt tag](https://github.com/mahrukh-uw/HateSpeechTrends/blob/master/images/system_architecture.png)

To handle the problem of hatespeech on Twitter I created this app on AWS because twitter doesn't have automatic content moderation algorithm .I downloaded different datasets from 
different sources, some of them are: t-davidson/hate-speech-and-offensive-language , https://files.pushshift.io/twitter/US_PoliticalTweets , https://github.com/ziqizhang/chase
Databases : Hate Speech Detection , Hate speech Twitter Annotations , US political tweets.
 I analzyed these datasets in batch processing mode.
I clean and pre-processed the data , after normalization I stored batches of structured data on S3, for Ingestion I used Apache Spark where text- processing and matching with Lexicon was performed. (Lexicon is a pre-defined dictionar or set of keywords created by some of the researchers and social media comapnies).
Cassandra was used for storing results or trends and lastly, I used Plotly|Dash to visualize trends.
I focused on detecting hate speech related key words using pre-defined dictionaries(lexicon). In other words, for each tweet, I performed a lookup into pre collected collections of hate speech keywords. I  also visualized the trends related to top users posting hate contents For example, the number of hateful posts in the last week and most frequent hate speech related keyword.

