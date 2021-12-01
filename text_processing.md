# **Text Processing Using Spark DataFrames**

Businesses want to get reviews on their goods and services but getting the review is not the issue. It’s the ability to extract and visualize analytics from review data that is most critical to a business’s success. For this tutorial, we’ll be using Spark DataFrames and the Yelp dataset to analyze business reviews.
This link will take you to the data that we will be working with. Download the necessary files to follow along in this tutorial.

## What is a DataFrame

A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs.

## Setting up the notebook to begin working in Spark Core

A SparkSession can be used to create aDataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files. To create a SparkSession, use the following builder pattern:

```python
from IPython.core.display import display,HTML
display(HTML("<style>.container {width:90% !important;}</style>"))
from pyspark.sql import SparkSession
spark = SparkSession \
 .builder \
 .master("local[*]") \
 .appName('spark app') \
 .getOrCreate()
sc = spark.sparkContext
```

## Assigning the data sets

Link to datasets: https://www.kaggle.com/yelp-dataset/yelp-dataset

```python
checkin = spark.read.json('yelp_academic_dataset_checkin.json')
review = spark.read.json('yelp_academic_dataset_review.json')
tip = spark.read.json('yelp_academic_dataset_tip.json')
user = spark.read.json('yelp_academic_dataset_user.json')
```

## Example 1

```python
Importing libraries and dependencies
from pyspark.sql.types import FloatType
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import ArrayType,StringType
```

Let's count how many users received more than 10,000 funny votes. We'll view the first five lines of the user data file using the .show() function to get a better view of the information that is available in the file such as column names and data format.

```python
user.show(5)

+-------------+---------------+---------------+----------------+--------------+---------------+---------------+---------------+-----------------+----------------+------------------+-----------------+----+-------------------+----+--------------------+-----+------+------------+------+--------------------+-------------------+
|average_stars|compliment_cool|compliment_cute|compliment_funny|compliment_hot|compliment_list|compliment_more|compliment_note|compliment_photos|compliment_plain|compliment_profile|compliment_writer|cool|              elite|fans|             friends|funny|  name|review_count|useful|             user_id|      yelping_since|
+-------------+---------------+---------------+----------------+--------------+---------------+---------------+---------------+-----------------+----------------+------------------+-----------------+----+-------------------+----+--------------------+-----+------+------------+------+--------------------+-------------------+
|         4.03|              1|              0|               1|             2|              0|              0|              1|                0|               1|                 0|                2|  25|     2015,2016,2017|   5|c78V-rj8NQcQjOI8K...|   17|Rashmi|          95|    84|l6BmjZMeQD3rDxWUb...|2013-10-08 23:11:33|
|         3.63|              1|              0|               1|             1|              0|              0|              0|                0|               0|                 0|                0|  16|                   |   4|kEBTgDvFX754S68Fl...|   22| Jenna|          33|    48|4XChL029mKr5hydo7...|2013-02-21 22:29:06|
|         3.71|              0|              0|               0|             0|              0|              0|              1|                0|               0|                 0|                0|  10|                   |   0|4N-HU_T32hLENLnts...|    8| David|          16|    28|bc8C_eETBWL0olvFS...|2013-10-04 00:16:10|
|         4.85|              0|              0|               0|             1|              0|              0|              0|                0|               2|                 0|                1|  14|                   |   5|RZ6wS38wnlXyj-OOd...|    4|Angela|          17|    30|dD0gZpBctWGdWo9Wl...|2014-05-22 15:57:30|
|         4.08|             80|              0|              80|            28|              1|              1|             16|                5|              57|                 0|               25| 665|2015,2016,2017,2018|  39|mbwrZ-RS76V1HoJ0b...|  279| Nancy|         361|  1114|MM4RJAeH6yuaN8oZD...|2013-10-23 07:02:50|
+-------------+---------------+---------------+----------------+--------------+---------------+---------------+---------------+-----------------+----------------+------------------+-----------------+----+-------------------+----+--------------------+-----+------+------------+------+--------------------+-------------------+
only showing top 5 rows
```

As we can see from using the .show() function, the data file contains a column labeled "compliment_funny". We'll use this column to pull the information we need. Let's take a look at what's in this column by using .select() then returning the first five lines using .show()

```python
user.select('compliment_funny').show(5)

+----------------+
|compliment_funny|
+----------------+
|               1|
|               1|
|               0|
|               0|
|              80|
+----------------+
only showing top 5 rows
```

The PySpark .filter() function can be used to filter lines based on a condition so we'll filter the "compliment_funny" column and set a condition with the .count() function to count all values with more than 10,000 votes.

```python
user.filter(user['compliment_cool'] >= 10000).count()

22
```

The output returned 2 users who received more than 10,000 votes.

## Example 2

We can find the most useful negative reviews using the review data file. Using the same technique above, we'll use the .show() function to view the first five lines of the data file.

```python
review.show(5)

+--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+
|         business_id|cool|               date|funny|           review_id|stars|                text|useful|             user_id|
+--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+
|ujmEBvifdJM6h6RLv...|   0|2013-05-07 04:34:36|    1|Q1sbwvVQXV2734tPg...|  1.0|Total bill for th...|     6|hG7b0MtEbXx5QzbzE...|
|NZnhc2sEQy3RmzKTZ...|   0|2017-01-14 21:30:33|    0|GJXCdrto3ASJOqKeV...|  5.0|I *adore* Travis ...|     0|yXQM5uF2jS6es16SJ...|
|WTqjgwHlXbSFevF32...|   0|2016-11-09 20:09:03|    0|2TzJjDVDEuAW6MR5V...|  5.0|I have to say tha...|     3|n6-Gk65cPZL6Uz8qR...|
|ikCg8xy5JIg_NGPx-...|   0|2018-01-09 20:56:38|    0|yi0R0Ugj_xUx_Nek0...|  5.0|Went in for a lun...|     0|dacAIZ6fTM6mqwW5u...|
|b1b1eb3uo-w561D0Z...|   0|2018-01-30 23:07:38|    0|11a8sVPMUFtaC7_AB...|  1.0|Today was my seco...|     7|ssoyf2_x0EQMed6fg...|
+--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+
only showing top 5 rows
```

For this particular example, we'll consider reviews with less than a three-star rating to be negative and use that information to find the most useful reviews.

Begin by creating a table with only negative reviews rated below three stars by using the .filter() function on the "stars" column.

```python
negreviews = review.filter(review['stars'] < 3)
```

Use .select() to pull the "stars", "useful", and "text" columns from our new negreviews table. The "useful" column gives us a score so that the highest scored reviews are considered to be most useful.

Simply sort using the .short() function by "stars" then "useful" to get the text of the most useful reviews followed by .show() to view the output.

```python
df = negreviews.select("stars","useful","text").sort("stars","useful","text",
ascending=False).show(10)

+-----+------+--------------------+
|stars|useful|                text|
+-----+------+--------------------+
|  2.0|  1241|Dinner for 1.

- ...|
|  2.0|   538|I was disappointe...|
|  2.0|   514|My wife and I wal...|
|  2.0|   507|I live in mesa, w...|
|  2.0|   467|I went to ABC dur...|
|  2.0|   354|Do not use this s...|
|  2.0|   338|I have debated ab...|
|  2.0|   333|I ordered H2O Con...|
|  2.0|   317|Went here w/famil...|
|  2.0|   292|I went here becau...|
+-----+------+--------------------+
only showing top 10 rows
```

While the above output gives us what we're looking for, I'd like to be able to view the entire text of the reviews to see what made them so useful. Instead of using the .show() function, which prints results, we'll use the .take() function to return a list of rows so we can view the text rows in their entirety.

```python
df = negreviews.select("stars","useful","text").sort("stars","useful","text",ascending=False).take(10)
print(df)
```

```python
[Row(stars=2.0, useful=1241, text='Dinner for 1.\n\n- Preface\nI went to Amy\'s Baking Company with an open mind. After all, reality TV does have a tendency to make things appear more extreme. However, I was nervous. What if Samy smelled a Yelper?  There were many things I was afraid to order. Damn you Kitchen Nightmares. While the menu is filled with gnocchi choices, I deviated since I didn\'t want to eat non-refrigerated, packaged gnocchi. The burger was out since it looked like a greasy mess. And I didn\'t like the idea of eating raw dough, so no pizza. They were out of soup that night, which I found mildly annoying. In order to write an objective review, it was important to me to remain a neutral, regular customer, with no unnecessary provoking. \n\n- Ambiance (4/5)\nABC is clean and chic, but quite empty inside. I was 1 of 3 tables. Perhaps it would have been excusable, except the place next door was quite busy.\n\n- Service (3/5)\nSamy did most of the work with the only waitress in the restaurant unmotivated; Samy even had to ask her to refill my water. If Samy took the tip, I wouldn\'t have faulted him. He was professional all night, bordering friendly dare I say? Service was also fast. I did catch a glimpse of Amy, who had an iPhone earphone on--either listening to music or on the phone in the kitchen.  She seemed quite calm and collected. Every time Samy came to the table with food, he would tell me how delicious it would be. Also, instead of asking how the food was, he would ask "it\'s delicious, isn\'t it?" So even when I had some serious concerns, I just let it pass. I\'m not really a confrontational person. Just come home and Yelp about it!  \n\n- Bread (N/A)\nConsidering the upscale vibe ABC is going for, I was quite surprised not to receive any bread.\n\n- Lemon Garlic Hummus, $5 (2/5)\nThe lemon garlic hummus had good Mediterranean flavor, but I think Amy must have added a "drizzle" of oil on top, because it was too oily. Of the bits I was able to taste, it was actually decent, but the oiliness really put me off and deterred me from finishing.\n\n- Herbed Flatbread, $5 (5/5)\nI felt deceived. I ordered the hummus expecting there would be some sort of bread or cracker included. So, when Samy asked casually about the flatbread with the hummus, I assumed it was included. It wasn\'t. The flat bread comes in a huge portion, about 10 big pieces. Quite good!  Garlicky, toasty on the top, but soft in the middle. I\'d certainly come back for the flatbread. Ironically, the one thing I didn\'t intend to order turned out to be the best.\n\n- Pesto Spinach Ravioli, $16 (2/5)\nGordon Ramsey dissed the ravioli on the show, but he seemed to make more a hubbub about the fact that they were frozen. I highly suspect these were the same frozen raviolis that Gordon got since fresh raviolis have a different texture. These raviolis were also quite flavorless, which is ironic considering the really salty and oily (sensing a theme?) sauce they were drenched in. I was "foraging" for raviolis. The spinach was cooked quite nicely, and I do appreciate the fresh tasting prosciutto and mushrooms. The concept was nice. If the sauce was a bit less oily, Amy used a little bit less salt, and we had homemade raviolis, this would\'ve been a passable dish.    \n\n- Key Lime Pie, $10 (1/5)\nABC had been criticized due to revelations that the desserts were store bought and not actually homemade. Who cares? I\'m sure this kind of thing happens far more than we know. To my severe disappointment, it was not at all good. It had a bit more of an oily foamy consistency. It was also much more sour than key lime pie should be, and the crust tasted a bit burnt. And what\'s with the edible glitter! I think this pie is the perfect representation of ABC--glittery on the outside, but once you dip your fork in past the surface, you find some unappetizing elements.  \n\n- Value (2/5)\nConsidering the prices ABC is charging, I expected a fine dining experience. Unfortunately, the food is just not up to par. Even if the key lime pie had tasted good, $10 for a piece of pie? C\'mon... it\'s only $8 at Joe\'s Stone Crab in Miami!! The portions are large--Samy mentioned that everything they do is large. And while quantity might be important to some, quality is more important to me.  \n\n- Final Word\nI\'m disappointed that Samy and Amy did not take Gordon Ramsey\'s advice. With instruction, Chef Amy could serve good dishes. I would love to see the owners take a chill pill and fix things. If I hadn\'t seen the way they treated customers on the show, I would have felt sorrier for them.  They looked quite rundown by all the attention. To the reviewers who keep leaving 1 star reviews without having visited the place, stop. Go try it; then let your Yelp review do the talking. The other 2 tables there looked like regulars, and they seemed to like the food. Not sure if they\'ve lost their taste buds, but to each his own. To the owners--swallow your pride and figure it out!  I hope I can come back one day when your food is better.  Good luck.'), Row(stars=2.0, useful=538, text="I was disappointed to see that they've changed their menu from pastas/pizzas to sandwiches/salads. Apparently, they're trying serve healthier, easier-to-make foods.\n\nMy boyfriend and I got the cheese panini and caramel cheesecake. The panini tasted like any other you could make at home and the salad was drenched in its dressing.\n\nThe cheesecake was good ONLY if you had the caramel drizzle and crust in the same bite. Otherwise, the middle of the cheesecake itself was bland. \n\nSamy and the waitress who served us were really friendly and at the end of our meal, Samy came by to thank us and shake our hands."), Row(stars=2.0, useful=514, text="My wife and I walked in with an open mind to give Amy's Baking Company a try even though I do agree with Amy herself, don't feed off what everyone is saying about the restaurant through the media or what's posted online. GO IN AND TRY IT YOURSELF!! \n\nI personally was hoping that the food would be amazing. Here are my thoughts as followed.\n\nFirst, the restaurant looks great. Very Clean but Samy Bouzaglo gives me and my wife the creeps.\n\nSecond, the food and desert were good but nothing extraordinary.\n\nIt is a pricey restaurant but not worth my money to return.\n\nDoesn't give discounts to Current Military Servicemen."), Row(stars=2.0, useful=507, text="I live in mesa, work in scottsdale decided to give this place a try. I don't have cable so I must have missed what happened, hope they have it on YouTube. \n\nUnfortunately the food was not so good (and I am not a picky eater! As long as it has cheese it's all good) but the lasagna was tasteless. The dessert was great! Thank you! And that's the reason for the two stars. Waiter was great. Owner not so nice. The vibe was a bit off, felt like they didn't want us there!\n\nBut I would like to say to my other wonderful YELPERS that calling these people names ...the c*** word etc....lowers us to their same level of insanity.\nSo I will do my part and no longer make them money, simply don't eat there!\n\nBut please please let's be civil,adult, educated people and not bullies!!!!\nThey are still people...someone wrote that they should be shot?? Why?? Over bad attitude? Lots of people would be lined up and executed including some of you and maybe me....who knows!? \nAll I'm saying is this is getting kinda extreme...\nSome reviews make me laugh till I cry and smile, but some are just vile and make me deeply sad.\n\nLet that marinate......"), Row(stars=2.0, useful=467, text='I went to ABC during the filming of Kitchen Nightmares, so that should tell you something right there. I already knew all about the hoopla surrounding this place and decided it was an experience not to be missed. Plus, I love Gordon Ramsay. \n\nThe first night of reservations was a cluster-f. I never made it inside and was given a reservation for the following evening instead. In their defense, they had a problem with the POS that seemed to be like a ... nightmare. Ironic, no? We decided to have dinner on the patio next door and watched a very dramatic night unfold at ABC.\n\nThe second night, we made it in. First off, let me just say that since this dining experience was rather atypical, I felt like some of the other diners were purposely trying to find things wrong with their meal. Aside from the fact that it\'s awkward to stuff your face with pasta in front of a camera, I tried my best to be impartial.\n\nShortly after we were seated, Gordon eighty-sixed the ravioli for being of the frozen, store bought variety. No surprise there considering the reviews. I had already known to steer clear. I played it safe and ordered the bolognese. It was fine, but nothing to come back for. On the other hand, the pizza and humus were actually pretty darn good. None of us found any reason to complain about the food. In fact, I actually really enjoyed my boyfriend\'s sweet and spicy pasta rustica. I believe Amy makes the sweet and spicy marmalade cream sauce from scratch. It was quite good, if you like a bit of kick. \n\nMy table was pretty agreeable until the bill arrived. I think this is where ABC struggles: customer service. If the customer has a problem, for some reason it is not smoothed over. The staff and owners go immediately on the defensive. You literally have to fight to make it right. It seemed very unreasonable to me that we had to point out to them what their menu stated. It didn\'t matter that we ordered it or double checked when it arrived to make sure that our order was correct. When the bill arrived and it was still incorrect, we were told that we were wrong. We had to point out that they were wrong by using their menu as evidence. Ridiculous.   \n\nAs we were waiting for the corrected bill, Gordon brought to light some sketchy practices concerning the tip money. It seems the owners are skimming from the waitresses tips. I\'m not exactly sure what was going on, but there was an argument where Gordon threw out the term "slave labor." Amy then came out to add that the cameras weren\'t around to see the waitresses when they were just standing around, as if that were a good enough excuse for the tip controversy. A few of the other patrons were pretty surprised and dismayed to find out how little the waitresses were paid. Hopefully, that is something that will be resolved because of the show. I would hate to think I tipped the owner 20% to argue with him over the size of pizza my dining companion ordered. Either way, the customer service and the behavior of the owners was a HUGE turnoff. \n\nIn conclusion, ABC\'s biggest problem is customer service. There is none. The waitresses are scared, under-appreciated young girls, just trying to make it through the night. The food, for the most part, is actually pretty good. At least when Gordon is around kicking ass. I can\'t wait to see how this place is turned around because of the show. Personally, I hope the owners realize you catch a lot more bees with honey.'), Row(stars=2.0, useful=354, text='Do not use this service.  Or if you do, be very diligent about your ticketing with them, or it may cost you hundreds more dollars than you\'ve expected.  \n\nHere\'s my story: I had pre-purchased two tickets for travel with Suburban Express, for travel dates two weeks apart.  Apparently, on the earlier date of travel, I accidentally used the ticket for the later date.  An honest mistake, right?  I didn\'t catch it, and the driver said nothing, though he seemed to have looked at my ticket.  \n\nA month later, I get an email informing me of my infraction.  Not only would I be charged $30 for the ticket, I would also be charged $100 "as stated in the terms and conditions," for ticket fraud.  Ticket fraud?!\n\nI contacted the company, gave them the reservation numbers for both tickets, trying to prove to them that I was a legitimate customer who had made an honest mistake.  The customer service representative  said they\'d review my case and get back to me.  A few weeks later, still not having heard anything from them, I called to check.  They said my case had been denied and there was nothing--nothing--I could do about it.\n\nSure, I\'d read the terms and conditions and knew about the fee; sure, I should\'ve looked more carefully at the ticket I\'d brought with me to the shuttle.  But don\'t we all make honest mistakes?  Don\'t expect to make a mistake with Suburban Express. People who make mistakes are considered frauds.\n\nUp until this experience, I\'d had a decent time with this company.   But now I will choose from among the many other shuttle companies in C-U that offer the same service.    \n\nIf you choose to ride with them, just be REALLY careful about which ticket you take with you and don\'t expect your rock-solid proof of purchase to mean anything.'), Row(stars=2.0, useful=338, text='I have debated about writing since i heard about the Joel/Amy battle, but decided to put in my two cents.  \n\nI first went to Amy\'s quite a few years ago when they first opened.  I had heard from several family members who had happened by and bought dessert.  Knowing how big of a dessert fiend I was the alerts kept coming.  From my first experience I was sold.  It was not just the desserts though the other food was very good and I brought several people in to dine there.\n\nIn general most people thought it was overpriced, but my husband and I don\'t normally let that stop up unless it is obscene.  My employees even bought me a red velvet cake from there and got me a gift certificate.\n\nWell for whatever reason we didn\'t go there for a little while and when we did go there was a sign saying that it was closed as Amy had health problems.  We were very sad.  I had met Amy several times and I will tell you she is passionate about her food and when encountering foodies she is very excited to talk about it.  \n\nI watched the website and then one day they were back.  The day we went we had just spent the night on the far west side of the valley after going to party that lasted very late and had a bit of alcohol.  We live on the east side and I was really excited that we could go to Amy\'s on the way around the 101.  \n\nWhen we got there it was relatively early on a Sunday and we were alone but for one other table.  We quickly went to work with the menu and ordered several things knowing we would have wonderful food to take home.  \n\nWhere things went amiss is when we went to play a game while waiting for our food.  Amy\'s husband came over and to be honest I can\'t remember exactly what he said (I am forever mortified by the whole experience) but we were essentially told they were not "that kind of place" and we were not welcome.  We had just gotten beverages and I told my husband just to pay for it and let\'s go.  I am not a public scene kind of person and I was very upset.  The server didn\'t want to come near us as I recall she wouldn\'t make eye contact and even hid behind the counter.   Finally the husband rang it up and out we went. \n\nAt that point I didn\'t want to eat anywhere I just wanted to go home.  I have never even discussed this with anyone but my family that saw me after when we picked up our dogs.  (Dogs that once upon a time sat on Amy\'s patio while we enjoyed our food.)   My husband called them afterwards. To my further mortification he spoke directly with Amy and she defended her husband and their decisions about what goes on at their restaurant.  \n\nNow I don\'t disagree with them saying no games at their restaurant.  It is their place and that means their rules, but it was just the way it was handled.  You can\'t go around making people feel less than and in Amy\'s response to Joel it was very clear again that there are some strong prejudices occurring.  \n\nObviously I won\'t ever go back and the gift certificate long sits in a drawer unused.  There are places in town that are better and many more that are worse, but overall it is customer service that ultimately will make the difference.'), Row(stars=2.0, useful=333, text='I ordered H2O Concepts water system in Sept 2014. First, I met with the salesperson Karen. She came with the typical overview of product and their certifications. I already did my homework so i knew i wanted to give it a try. I was given most discounts that were promised online. I had to remind Karen I also received a factory discount. I wont have gotten it otherwise. H2O System was installed 2 days later on a Thursday and Tom the installer was very good. He gave me the literature and i signed a few papers. I was told that it would take a few hours for salt to clear the pipes. I still had a salty taste so i called Friday for them to return. I called the home office and the person was very nice but told me no one was available until Monday. I called early Friday afternoon and no one could help me !! I just paid over $4000 and no one could help me ???  I had to go all weekend into Monday afternoon with salt water. I called my salesperson Karen for help on Friday and she said call the office and that she wasnt techie. She was not willing to call people in my behalf to help me out and never returned any of my followup calls. A hands off salesperson once she has you sign on dotted line. Very unprofessional. I called and left a message at home office  if i didnt get a service call to remedy my issue on monday I would cancel my order since i had 72 business hour protection to do so. A techie Ken came Monday afternoon who was very polite and flushed the system rebooted it and tested every sink for salt or bad chemicals. It all checked out. Both techies Tom and Ken as well as home office receptionist were very professional and helpful.\nAs for the salesperson Karen. Very unprofessional and only out for a signed contract with absolutely NO CUSTOMER SUPPORT DESIRE. Its a family owned business clearly lacking sales professionals that care to get involved when the customer who just ordered a system needs help.\nKaren was a hands off sales person that never called me back. Her professional integrity is questionable. She really made my experience with H2O Concepts a poor one. All she had to do is get involved and follow up with me. Neither was done.'), Row(stars=2.0, useful=317, text='Went here w/family last year (b4 "Nightmares"). Food was intolerable. Dessert was decent enough. Didn\'t leave tip, because family members informed me about owners taking the tips.  So, to clarify to person from Santa Clara (me=born & raised in CA),\nin CA IT IS ILLEGAL TO TAKE STAFF TIPS-BUT NOT IN AZ (of course). The hourly wage in a lot of "red" states is very low. Barely above Federal minimum. Unlike CA.'), Row(stars=2.0, useful=292, text='I went here because it was on Gordon Ramsay\'s show.\n\nMy roommate roommate and I are 28 y/o males.\n\nI thought the show might be kind of staged... but no. I saw the owner (the guy) be very rude to a costumer wanting a specific kind of cake. It was entertaining and probably made the experience worthwhile \n\nThe menu did not appeal to either of us. Not much I wanted to eat, but that could just be our style in food.\n\nI ordered the burger just like Ramsay did on the show. He was right. It was way too "juicy" and the bun fell apart. Gross. It had a bad flavor and the burger "juice" was all over the place.\n\nI would give a C-. My roommate had a C- meal as well. \n\nIt was a fun experience, but food could be found better almost anywhere!')]
```

## Example 3

Importing libraries and dependencies

```python
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.functions import hour, desc
from pyspark.sql.functions import trim,ltrim,rtrim
```

Using the checkin data file, we can return an easily readable view of the DataFrame schema using the .printSchema() function to find out what time had the least check-ins. We'll start with two column names, "business_id" and "date", to find the information we need.

```python
checkin.printSchema()

root
 |-- business_id: string (nullable = true)
 |-- date: string (nullable = true)

```

Next, we'll extract check-in times using the .split() function on comma with the result of a list of strings.

```python
split = udf(lambda x: x.split(','),ArrayType(StringType()))
```

Using .select(), we'll return the "business_id" column, apply the split function to the "date" column, then create a new .alias() column labeled "dates".

```python
df_hour=checkin.select('business_id',split('date').alias('dates'))
df_hour.show()

+--------------------+--------------------+
|         business_id|               dates|
+--------------------+--------------------+
|--1UhMGODdWsrMast...|[2016-04-26 19:49...|
|--6MefnULPED_I942...|[2011-06-04 18:22...|
|--7zmmkVg-IMGaXbu...|[2014-12-29 19:25...|
|--8LPVSo5i0Oo61X0...|[2016-07-08 16:43...|
|--9QQLMTbFzLJ_oT-...|[2010-06-26 17:39...|
|--9e1ONYQuAa-CB_R...|[2010-02-08 05:56...|
|--DaPTJW3-tB1vP-P...|[2012-06-03 17:46...|
|--DdmeR16TRb3LsjG...|[2012-11-02 21:26...|
|--EF5N7P70J_UYBTP...|[2018-05-25 19:52...|
|--EX4rRznJrltyn-3...|[2010-02-26 17:05...|
|--FBCX-N37CMYDfs7...|[2010-05-31 07:57...|
|--FLdgM0GNpXVMn74...|[2012-10-23 18:47...|
|--GM_ORV2cYS-h38D...|[2011-09-11 18:16...|
|--Gc998IMjLn8yr-H...|[2014-07-01 01:20...|
|--I7YYLada0tSLkOR...|[2014-11-07 00:51...|
|--KCl2FvVQpvjzmZS...|[2011-07-29 16:53...|
|--KQsXc-clkO7oHRq...|[2010-05-02 23:57...|
|--Rsj71PBe31h5Ylj...|[2011-12-15 18:09...|
|--S62v0QgkqQaVUhF...|[2010-12-25 07:04...|
|--SrzpvFLwP_YFwB_...|[2011-02-10 03:51...|
+--------------------+--------------------+
only showing top 20 rows

```

The explode function returns a new row for each element in the array. It can be applied to the "dates" column in a new column with the pre-processed values labeled "checkin_date".

```python
df_hour_exploded = df_hour.withColumn('checkin_date',explode('dates'))
df_hour_exploded.show()

+--------------------+--------------------+--------------------+
|         business_id|               dates|        checkin_date|
+--------------------+--------------------+--------------------+
|--1UhMGODdWsrMast...|[2016-04-26 19:49...| 2016-04-26 19:49:16|
|--1UhMGODdWsrMast...|[2016-04-26 19:49...| 2016-08-30 18:36:57|
|--1UhMGODdWsrMast...|[2016-04-26 19:49...| 2016-10-15 02:45:18|
|--1UhMGODdWsrMast...|[2016-04-26 19:49...| 2016-11-18 01:54:50|
|--1UhMGODdWsrMast...|[2016-04-26 19:49...| 2017-04-20 18:39:06|
|--1UhMGODdWsrMast...|[2016-04-26 19:49...| 2017-05-03 17:58:02|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2011-06-04 18:22:23|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2011-07-23 23:51:33|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2012-04-15 01:07:50|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2012-05-06 23:08:42|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2012-06-08 22:43:12|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2012-08-06 23:20:52|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2012-08-19 18:30:44|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-01-27 23:49:51|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-03-01 01:22:29|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-03-23 21:53:47|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-03-24 01:11:51|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-05-20 00:12:25|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-06-29 22:50:57|
|--6MefnULPED_I942...|[2011-06-04 18:22...| 2013-07-01 15:58:04|
+--------------------+--------------------+--------------------+
only showing top 20 rows
```

To find the time with the least check-ins, the hour function can be used to extract the hours of a given date as an integer. The data needs to be trimmed of empty spaces so we'll use the trim function to trim the spaces from both ends for the string column. Then creating another alias with a column labeled "finaldates", we'll use .groupBy(), .count(), then .sort() to return a list of hours and their count in ascending order to get the result we're looking for.

```python
df_hour2=df_hour_exploded.select(trim(df_hour_exploded['checkin_date']).alias('finaldates')).groupBy(hour('finaldates').alias('final')).count().sort("count",ascending=True).show(24)

+-----+-------+
|final|  count|
+-----+-------+
|   10|  88486|
|    9| 100568|
|   11| 111769|
|    8| 151065|
|   12| 178910|
|    7| 231417|
|   13| 270145|
|    6| 321764|
|   14| 418340|
|    5| 485129|
|   15| 617830|
|    4| 747453|
|   16| 852076|
|   17|1006102|
|    3|1078939|
|   21|1238808|
|   22|1257437|
|   18|1272108|
|   23|1344117|
|   20|1350195|
|    2|1411255|
|    0|1491176|
|   19|1502271|
|    1|1561788|
+-----+-------+
```

From what we can see in the return, 10 pm was had the lowest occurence of check-ins.

## Example 4

```python
Importing libraries and dependencies
from pyspark.ml.feature import *
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from pyspark.sql.functions import *
import pandas as pd
import string 
import re
```

In Spark SQL caching is a common technique for reusing computation. It has the potential to speedup other queries that are using the same data so we'll use this function to take a look at the review data file we used earlier.

```python
review.cache()

DataFrame[business_id: string, cool: bigint, date: string, funny: bigint, review_id: string, stars: double, text: string, useful: bigint, user_id: string]
```

Pre-processing the text data

Let's find the most common words that are unique to positive and negative reviews. First, we need to clean the text of the reviews by removing any punctuation or numbers using the following function.

```python
# remove punctuation
def remove_punct(text):
    regex = re.compile('[' + re.escape(string.punctuation) + '0-9\\r\\t\\n]')
    nopunct = regex.sub("", text) 
    return nopunct
```

The "stars" column must be relabelled so that any reviews with three stars or above will be positive, anything else is considered to be negative. We also remove punctuations and numbers before tokenizing the text.

```python
# binarize rating
def convert_rating(rating):
    rating = int(rating)
    if rating >=3: return 1
    else: return 0
```

We have to convert these functions to PySpark's user-defined functions then apply them to the text and rating columns upon querying from the whole 'review' collection.

```python
# udf
punct_remover = udf(lambda x: remove_punct(x))
rating_convert = udf(lambda x: convert_rating(x))
# apply to review raw data
review_df = review.select('review_id', punct_remover('text'), rating_convert('stars'))
review_df = review_df.withColumnRenamed('<lambda>(text)', 'text')\
                     .withColumn('label', review_df["<lambda>(stars)"].cast(IntegerType()))\
                     .drop('<lambda>(stars)')
review_df.show(5)

+--------------------+--------------------+-----+
|           review_id|                text|label|
+--------------------+--------------------+-----+
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|
|2TzJjDVDEuAW6MR5V...|I have to say tha...|    1|
|yi0R0Ugj_xUx_Nek0...|Went in for a lun...|    1|
|11a8sVPMUFtaC7_AB...|Today was my seco...|    0|
+--------------------+--------------------+-----+
only showing top 5 rows
```

Next, we begin tokenizing the sentences into words. The words are then processed to remove stop words such as prepositions and articles so we can keep more meaningful words in the corpus.


```python
tok = Tokenizer(inputCol="text", outputCol="words")
review_tokenized = tok.transform(review_df)
# remove stop words
stopword_rm = StopWordsRemover(inputCol='words', outputCol='words_nsw')
review_tokenized = stopword_rm.transform(review_tokenized)
review_tokenized.show(5)

+--------------------+--------------------+-----+--------------------+--------------------+
|           review_id|                text|label|               words|           words_nsw|
+--------------------+--------------------+-----+--------------------+--------------------+
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|[total, bill, for...|[total, bill, hor...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|[i, adore, travis...|[adore, travis, h...|
|2TzJjDVDEuAW6MR5V...|I have to say tha...|    1|[i, have, to, say...|[say, office, rea...|
|yi0R0Ugj_xUx_Nek0...|Went in for a lun...|    1|[went, in, for, a...|[went, lunch, ste...|
|11a8sVPMUFtaC7_AB...|Today was my seco...|    0|[today, was, my, ...|[today, second, t...|
+--------------------+--------------------+-----+--------------------+--------------------+
only showing top 5 rows

```

The explode function comes in handy once again by splitting each word into its own row.

```python
dfwords_exploded = review_tokenized.withColumn('words',explode('words_nsw'))
dfwords_exploded.show(50)

+--------------------+--------------------+-----+--------+--------------------+
|           review_id|                text|label|   words|           words_nsw|
+--------------------+--------------------+-----+--------+--------------------+
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   total|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|    bill|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|horrible|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0| service|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|      gs|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|  crooks|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|actually|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   nerve|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|  charge|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|      us|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|        |[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|        |[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   pills|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0| checked|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|  online|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   pills|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|        |[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   cents|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   avoid|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|hospital|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|     ers|[total, bill, hor...|
|Q1sbwvVQXV2734tPg...|Total bill for th...|    0|   costs|[total, bill, hor...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   adore|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|  travis|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|    hard|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   rocks|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|     new|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   kelly|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|cardenas|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   salon|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|        |[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|      im|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|  always|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|     fan|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   great|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1| blowout|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|stranger|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|  chains|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   offer|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1| service|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1| however|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|  travis|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   taken|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|flawless|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1| blowout|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   whole|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|     new|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|   level|[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1|        |[adore, travis, h...|
|GJXCdrto3ASJOqKeV...|I adore Travis at...|    1| traviss|[adore, travis, h...|
+--------------------+--------------------+-----+--------+--------------------+
only showing top 50 rows
```

Since we're looking for the most used words from positive and negative reviews, creating two separate tables will be the best way to find unique words. Having labeled all positive reviews 1 and all negative reviews as 0, we can create a filter with a condition to pull rows from rows labeled as such.

```python
pos = dfwords_exploded.filter(dfwords_exploded['label'] >= 1)
neg = dfwords_exploded.filter(dfwords_exploded['label'] < 1)
```

Using .select(), .groupBy(), .count(), then .sort(), we can organize the most commonly used words in positive and negative reviews. This information isn't quite helpful since there is a lot of commonality between the tables. 

```python
posdf = pos.select('words','label').groupBy("words",'label').count().sort("count",ascending=False)
posdf.show(51)

+----------+-----+--------+
|     words|label|   count|
+----------+-----+--------+
|          |    1|14202155|
|      good|    1| 2625913|
|     great|    1| 2609696|
|     place|    1| 2497629|
|      food|    1| 2424101|
|      like|    1| 1645870|
|   service|    1| 1641627|
|      time|    1| 1592364|
|       get|    1| 1478976|
|       one|    1| 1457249|
|    really|    1| 1333878|
|      back|    1| 1308397|
|        go|    1| 1225502|
|      also|    1| 1154973|
|      best|    1| 1028499|
|      nice|    1| 1023697|
|      love|    1|  961172|
|    always|    1|  943830|
|      well|    1|  912762|
|       got|    1|  880872|
|  friendly|    1|  876264|
|        us|    1|  839028|
|definitely|    1|  831299|
|       ive|    1|  830202|
|     staff|    1|  826464|
|        im|    1|  805946|
|   amazing|    1|  801766|
|    little|    1|  784466|
|      dont|    1|  782264|
|      even|    1|  766417|
|       try|    1|  765697|
| delicious|    1|  759596|
|   chicken|    1|  710060|
|     first|    1|  705436|
|   ordered|    1|  703766|
|restaurant|    1|  700487|
|      came|    1|  699878|
|      come|    1|  675666|
|      much|    1|  659158|
|      menu|    1|  650486|
| recommend|    1|  629307|
|    pretty|    1|  626777|
|      made|    1|  624765|
|     didnt|    1|  612480|
|      went|    1|  612005|
|     order|    1|  611991|
|experience|    1|  597104|
|    people|    1|  593983|
|      make|    1|  590378|
|       new|    1|  549911|
|everything|    1|  542821|
+----------+-----+--------+
only showing top 51 rows
```

```python
negdf = neg.select('words','label').groupBy("words",'label').count().sort("count",ascending=False)
negdf.show(51)

+----------+-----+-------+
|     words|label|  count|
+----------+-----+-------+
|          |    0|6508012|
|      food|    0| 850986|
|       get|    0| 743018|
|     place|    0| 722721|
|       one|    0| 702901|
|   service|    0| 700228|
|      like|    0| 695834|
|      time|    0| 688465|
|      back|    0| 677016|
|      good|    0| 546356|
|        us|    0| 545107|
|        go|    0| 518269|
|      even|    0| 513608|
|      dont|    0| 497249|
|      said|    0| 486334|
|     never|    0| 468583|
|     didnt|    0| 464763|
|      told|    0| 449084|
|       got|    0| 435326|
|     order|    0| 392212|
|      came|    0| 371292|
|     asked|    0| 365107|
|      went|    0| 358313|
|    really|    0| 353731|
|   ordered|    0| 345526|
|   minutes|    0| 339588|
|    people|    0| 324264|
|        im|    0| 322198|
|     first|    0| 305395|
|     going|    0| 289855|
|      know|    0| 287407|
|   another|    0| 274269|
|      also|    0| 273153|
|      come|    0| 269201|
|  customer|    0| 264270|
|       two|    0| 262327|
|       bad|    0| 260052|
|      took|    0| 259706|
|       way|    0| 254280|
|    better|    0| 247543|
|experience|    0| 247219|
|      much|    0| 246824|
|      take|    0| 245479|
|restaurant|    0| 243698|
|     still|    0| 243494|
|      make|    0| 243323|
|    called|    0| 242923|
|      give|    0| 242364|
|      well|    0| 241236|
|     great|    0| 234682|
|       day|    0| 232954|
+----------+-----+-------+
only showing top 51 rows

```

Create aliases for the two tables we're working with. The alias, like in SQL, allows you to distinguish where each column is coming from.
Reference

```python
postable = posdf.alias('postable')
negtable = negdf.alias('negtable')
```

Using a leftanti join returns only columns from the left dataset for non-matched words. This is how we'll return the most used unique words from the tables.

```python
uniquepos = postable.join(negtable, on='words', how='leftanti').sort("postable.count",ascending=False).show(50)

+-------------------+-----+-----+
|              words|label|count|
+-------------------+-----+-----+
|              eloff|    1|  289|
|        ahhhhmazing|    1|  210|
|               jabz|    1|  196|
|          yummmmmmm|    1|  159|
|          greatwill|    1|  156|
|             fixler|    1|  155|
|            yumthey|    1|  145|
|          ahhmazing|    1|  142|
|deliciousdefinitely|    1|  138|
|                ceg|    1|  137|
|          heartwood|    1|  137|
|               emme|    1|  127|
|             karved|    1|  120|
|              artur|    1|  119|
|              safak|    1|  110|
|         wellhighly|    1|  108|
|       shutterbooth|    1|  108|
|             sidell|    1|  106|
|           perfects|    1|  103|
|           dlicious|    1|  101|
|            crémeux|    1|  101|
|          delizioso|    1|   99|
|          areagreat|    1|   98|
|             yumfor|    1|   96|
|                imr|    1|   96|
|     serviceamazing|    1|   95|
|             exquis|    1|   93|
|            koshari|    1|   93|
|               jayd|    1|   93|
|               insp|    1|   91|
|          heavenlyi|    1|   91|
|         needsthank|    1|   90|
|        greathighly|    1|   90|
|            hobgood|    1|   90|
|         deeeeelish|    1|   89|
|          merveille|    1|   88|
|              hubba|    1|   86|
|          ahmazeing|    1|   86|
|             omalza|    1|   86|
|      scrumptiously|    1|   85|
|        wordamazing|    1|   84|
|    delicioushighly|    1|   83|
|            wooffle|    1|   83|
|        familythank|    1|   83|
|              trego|    1|   83|
|              saura|    1|   82|
|   experiencehighly|    1|   82|
|        knifeshaved|    1|   81|
|             hiroba|    1|   81|
|       deliciousill|    1|   80|
+-------------------+-----+-----+
only showing top 50 rows
```

```python
uniqueneg = negtable.join(postable, on='words', how='leftanti').sort("negtable.count",ascending=False).show(50)

+--------------------+-----+-----+
|               words|label|count|
+--------------------+-----+-----+
|        unempathetic|    0|   43|
|           conartist|    0|   40|
|           leftnever|    0|   38|
|              voiers|    0|   30|
|           discusted|    0|   29|
|                fahw|    0|   29|
|        incompetenti|    0|   28|
|               gager|    0|   28|
|          unlawfully|    0|   28|
|             itworst|    0|   27|
|           apologyno|    0|   27|
|             ontracs|    0|   26|
|         irishjewish|    0|   26|
|                 amj|    0|   26|
|        herehorrible|    0|   26|
|              rebill|    0|   26|
|           hereworst|    0|   25|
|              theifs|    0|   24|
|              liarsi|    0|   24|
|           rudenever|    0|   24|
|                 ybt|    0|   24|
|       againhorrible|    0|   23|
|          horribledo|    0|   22|
|                tkps|    0|   22|
|            comenity|    0|   22|
|          excustomer|    0|   22|
|             horable|    0|   21|
|        horribledont|    0|   21|
|             suppost|    0|   20|
|      unacceptableif|    0|   20|
|        elseanywhere|    0|   20|
|            cobraron|    0|   20|
|         unempowered|    0|   19|
|             jeldwen|    0|   19|
|             frauded|    0|   19|
|           repossess|    0|   19|
|               nedia|    0|   19|
|        doublebilled|    0|   19|
|         autopayment|    0|   19|
|               emele|    0|   18|
|     disgustingnever|    0|   18|
|           pmmelissa|    0|   18|
|horriblehorribleh...|    0|   18|
|              sabans|    0|   18|
|       compensationi|    0|   18|
|           fraudster|    0|   18|
|              scamwe|    0|   18|
|           edgeparks|    0|   17|
|       returningever|    0|   17|
|          placeavoid|    0|   17|
+--------------------+-----+-----+
only showing top 50 rows
```

Both sets of words appear to be what is expected when looking at negative or positive reviews but there were some words such as "eloff" that were questionable. Why would this be the number 1 most common word used in positive reviews? After doing some research, it was clear that many users included the name "eloff" in their responses, which is the name of one of the most popular businesses in the dataset. All in all, this method works well to extract the most common words that are unique to positive and negative reviews.

This post is a summary of the steps that yielded the best results for analyzing Yelp reviews.
