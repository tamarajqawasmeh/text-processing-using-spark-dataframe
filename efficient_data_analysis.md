# **Efficient Data Processing Using Spark DataFrames**

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
 .appName('My First Spark application') \
 .getOrCreate()
sc = spark.sparkContext
```

## Assigning the data sets

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

The PySpark .filter() function can be used to filter lines based on a condition so we'll filter the "compliment_cool" column and set a condition with the .count() function to count all values with more than 10,000 votes.

```python
user.filter(user['compliment_cool'] >= 10000).count()

22
```

The output returned 2 users who received more than 10,000 votes.

## Example 2

We can find the most useful positive reviews using the review data file. Using the same technique above, we'll use the .show() function to view the first five lines of the data file.

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

For this particular example, we'll consider only reviews with a five-star rating to be positive and use that information to find the top ten most useful positive reviews.
Begin by selecting the columns we are most interested in. In this case, we'll use .select() to pull the "stars", "useful", and "text" columns. The "useful" column gives us a score so that the highest scored reviews are considered to be most useful.
Simply sort using the .short() function by "stars" then "useful" to get the text of the top ten reviews followed by .show() to view the output.

```python
df = review.select("stars","useful","text").sort("stars","useful","text",
ascending=False).show(10)

+-----+------+--------------------+
|stars|useful|                text|
+-----+------+--------------------+
|  5.0|   358|The Wynn Hotel. O...|
|  5.0|   278|Wayne does a fant...|
|  5.0|   241|This is one of th...|
|  5.0|   215|Wenn man auf dem ...|
|  5.0|   215|I stopped by Echo...|
|  5.0|   210|After spending a ...|
|  5.0|   208|Why spend hundred...|
|  5.0|   207|Unser eher besche...|
|  5.0|   207|Auf unserer Rundr...|
|  5.0|   203|Auf unserer Casin...|
+-----+------+--------------------+
only showing top 10 rows
```

While the above output gives us what we're looking for, I'd like to be able to view the entire text of the reviews to see what made them so useful. Instead of using the .show() function, which prints results, we'll use the .take() function to return a list of rows so we can view the text rows in their entirety.

```python
df = review.select("stars","useful","text").sort("stars","useful","text",ascending=False).take(10)
print(df)

[Row(stars=5.0, useful=358, text='The Wynn Hotel. One word. Opulent. \n\nWynn, is a luxury resort and casino located on the Las Vegas Strip. Named after the casino developer Steve Wynn, this $2.7 billion resort together with Encore is the world\'s seventh-largest hotels and one of the finest in the world. \n\nHolding the title for the highest rated resort company in the world with 16 Forbes Travel Guide Five-Star Awards, AAA five diamond, Mobil five-star, Michelin five star, and Zagat Survey Top U.S. Hotel ratings, as well as one Michelin star for its restaurant Wing Lei, the first Chinese restaurant to receive a Michelin Star in the country. \n\nThe Wynn is a genre almost entirely their own. Talk about a blend of opulent creativity and seductive comfort. Wynn Hotel designer Roger Thomas calls it "Evocatect." The "architecture that evokes wonderful moments in people\'s lives." \n\nShare a moment alone or with someone special and walk through the sunlit Atrium, gaze at the immense beauty of the Parasols, dine at one of their Four-Star Award-winning restaurants, which holds more awards than any other independent resort in North America, shop at Chanel, Dior, or Louie Vuitton just to name a few, enjoy their epic casino gaming floor or just take a load off and stay at one of their exquisite rooms. \n\nWhatever you decide to do at the Wynn, their magnificent ambience and hospitality will create such an extraordinary emotional experience you will never forget.'), Row(stars=5.0, useful=278, text='Wayne does a fantastic Job, always on time, my carpets are beautiful when he leaves as is my grout that he scrubs back to the way it looked the day it was being put in.  He is always ON TIME, and his rates are fantastic,  He is the best I have ever used.  Give this company a try.  He is also owner/operator so it is always important to support local businesses.'), Row(stars=5.0, useful=241, text="This is one of the most interesting classic lofts complex this dude has seen.  In 1901, they built this place by Allegheny river to be the biggest cork factory in the world. This dude came to Pittsburgh in July 2016. Management here are courteous and pleasant. This place is only for rent; you can't own. They have high ceilings, red brick walls, gorgeous courtyard, lovely outdoor seats, fire pit and most importantly amazing neighbors.\n\nDogs are more than humans here. They have cool dogs shower at the basement. Maintenance staff are excellent here. Parking is ~$90/month. 1Br lofts ~$1700/month. Washer dryer are in unit. Concierge, business center and fitness are 24 hour. Dry cleaning is available. This dude will miss having dinners and tea with lovely neighbors by the courtyard watching the sunset. I could write pages of details. Ask if you have questions. \n\nNegatives: There is no perfect place. There are some ''fixable'' issues. I was hoping for a gas stove, digital thermostat, soft-closing cabinets and toilet seat. Also the elevator and corridors occasionally smell bad. \n\nGiving the enormous room, it would work perfectly for couples or hoarders here. I'm a minimalist and had a hard time filling my loft with stuff. Overall, it's a unique facility that is worth checking out. Five solid stars. My neighbors, I love you."), Row(stars=5.0, useful=215, text="Wenn man auf dem Strip zu Fuß unterwegs ist, sollte man es nicht versäumen, auch mal in die LINQ Promenade abzubiegen. Sie befindet sich gegenüber vom Caesars Palace zwischen dem Hotel The LINQ Resort and Casino und dem Flamingo Hotel.\n\nEröffnet wurde diese Flaniermeile am 1. April 2014 und ist meines Erachtens eine wirkliche Bereicherung für diese Gegend. Erbaut wurde das Areal von der Caesars Entertainment und mit der Sängerin Britney Spears feierlich eröffnet.\n\nDie schön gestaltete Promenade bietet den Besuchern 400 Geschäfte und Restaurants in einer 200.000 Quadratmeter großen Fläche an. Am Ende der Straße wartet auf die Gäste die Hauptattraktion, der High Roller, ein 167 Meter hohes Riesenrad und damit das größte der Welt.\n\nUnter anderem befinden sich auf der Promenade O'Shea's Casino, Chayo, The Haute Doggery Hot Dog shop, Off the Strip Bistro, In-N-Out Burger, Sprinkles Cupcakes, Starbucks, Ghirardelli, Yard House, Tilted Kilt and Flour & Barley. Shops include Photo & Go, Pier 30, Bella Scarpa, 12A.M. Run, Chilli Beans, Goorin Bros. and Ruby Blue. \n\nBei In-N-Out Burger wird man zu den Hauptzeiten sicherlich lange warten müssen, denn die Absperrbänder lassen dies erahnen. Sicherlich momentan die beliebteste Burgerkette an der Westküste. Bei unserem Besuch, kurz vor Halloween, waren überall gruselige Gestalten aufgestellt. Eine wirklich tolle Idee, die bei den Besuchern der Promenade sehr gut ankam.\n\nEine weitere Attraktion der LINQ Promenade ist die Brooklyn Bowl, ein Musikveranstaltungsort mit einer Kapazität von mehr als 2.000 Konzertbesuchern. Brooklyn Bowl verfügt darüber hinaus über ein Restaurant, 32 Bowlingbahnen, nächtliche Live-Auftritte und einen Nachtclub.\n\nEinen Besuch der LINQ Promenade kann ich absolut empfehlen."), Row(stars=5.0, useful=215, text="I stopped by Echo and Rig during lunch time on the weekend.  My server informed me that only brunch was being served, and that their regular menu is available at dinner times and on weekday lunches.  I really wanted to have a steak as I heard so much about their meats.  She talked to the kitchen and they were able to accommodate my request.  I went with the Spencer Steak.\n\nSpencer Steak ($29) - A Spencer steak is a 10oz. boneless rib eye.  Naturally I asked for my steak to be prepared medium rare.  All their steaks are cooked over red oak.  Some garlic chips and sauteed spinach accompanied my dish.  The steak tasted pretty good and was closer to being rare then medium rare.  My server explained to me that they do a true medium rare here and that many people are not used to it, and that she could put it back on the grill.  I thanked her but decided to eat the juicy steak as it was.  It was one of the better tasting steaks that I tried and surprised that the rib eye was under $30.\n\nBerry Shortcake ($6) - This consisted of berries, citrus shortcake, hibiscus syrup, whipped cream, and vanilla ice cream.  All the ingredients complemented each other really well and made for an incredible tasting dessert.  It was the perfect size to share with my part of 4.  All the desserts on the menu are priced at $6 and make for an affordable and delicious way to end the meal.\n\nOverall I was really impressed with my steak and dessert.  The service here was incredible as our server checked in on us throughout the course of the meal and made sure everyone was happy.  The next time I'm in the mood for steak and in Vegas, this will be the only place I will visit!"), Row(stars=5.0, useful=210, text="After spending a bunch of money on lunches and dinners during my stay in Las Vegas, it was good to find a place with reasonably priced breakfast options.  Located less then 2 miles from the Las Vegas City Center, this is an extremely convenient place to stop by for a quick bite to eat.   I know  everyone has been avoiding carbs, but for the few times that you do  choose to indulge in carbs, you will be hard pressed to find a better bagel.\n\nStandard bagel options include:\nAsiago\nBlueberry\nChocolate Chip\nCinnamon Raisin\nCinnamon Sugar\nCranberry\nEgg\nEverything\nFrench Toast\nGarlic\n9-Grain\nHoney Whole Wheat\nOnion\nPlain\nPoppy Seed\nPotato\nPretzel\nPumpernickel\nSesame Seed\n\nWhile the following shmears are available:\nPlain\nOnion and Chive\nSmoked Salmon\nBlueberry\nHoney Almond\nStrawberry\nGarden Veggie\nGarlic & Herb\nJalapeño Salsa\nReduced Fat Plain\nMaple\n\nThere are some gourmet bagel and special shmears available, depending on the season.  Honey almond is my personal favorite shmear.  Like all the other shmears it very creamy and light.  I love the taste of honey almond and it goes well with the cranberry bagel or cinnamon bagel.  \n\nOverall the bagels are soft, chewy and taste fresh whenever I have had them.  I've tried other chain brands and I have yet to find a better tasting bagel.  These bagels also remain fresh and taste great the following day.  I found this particular location to be clean and the staff to be friendly.  While other items such as sandwiches and egg sandwiches are available, I have yet to try those items.  I'll definitely be back whenever I'm in the Las Vegas area again."), Row(stars=5.0, useful=208, text="Why spend hundreds of dollars bribing douchebag VIP hosts and currying favor with fickle tourist divas in the hopes of MAYBE scoring the proverbial happy ending?  The Red Rooster is a sure bet: people come here to party and to get laid, and don't bullshit about it.  It's a club without all the pretense and poseurs!  \n\nRun by a genial middle-aged couple out of their sprawling private residence, the Red Rooster is just plain folks, drinking and dancing and getting frisky.  Definitely not a room full of Barbies and hardbodies... but on the plus side, you won't be judged, ridiculed or taken advantage of.  People are welcoming and friendly, and you can definitely get some here, if your standards aren't impossibly high (or even reasonably high, truth be told)!\n\nBut even if you're not into having sex with strangers in public, this is the best entertainment deal in Vegas, hands down!  The people watching is phenomenal, with reality shows, dramas and pornos playing out LIVE in every corner.  The liquor policy is BYOB, but you check your bottle in at the bar, and they label it with your name and will pour you drinks all night long as you wander around in a state of sensory overload.  I had a BLAST here, and I didn't have sex with anyone or engage in anything more risque than a little booty-shaking on the dance floor (yes, there is a dance floor and dj spinning Top 40 hits)!  The best part is, Looky-Lous (as I admittedly was) are not frowned upon, and it's prefectly cool to just have a drink and kick back and enjoy the show.\nIf you do want to participate, though, there are ample opportunities and extensive facilities -- a couples-only area upstairs, and an orgy room, indoor pool area, and myriad cozy little fuck-worthy nooks & crannies downstairs.\nDefinitely not for the shy or faint of heart... but if you're into a different kind of experience that takes the idea of recreation to the next level, try it out!"), Row(stars=5.0, useful=207, text='Unser eher bescheidenes Motel war in der Nähe des riesigen Wynn Las Vegas Hotel, auf das wir immer neidisch schauten. Es ist ein Luxushotel, das im Jahre 2005 eröffnet wurde und nach dem Immobilienmogul Steve Wynn benannt wurde. Gleich daneben ist das im gleichen Stil gebaute Schwesternhotel Encore.\n\nBei seiner Eröffnung war es das teuerste Hotel am Strip und auf der Welt. Mir war vor zwei Jahren nicht klar, dass dies ebenfalls ein Casino-Hotel ist, da das Wynn mit keinen von der Straße zu bewundernden Attraktionen Besucher anlockt. Sehr sehenswert ist der "Lake of Dreams", auf den Bilder projiziert werden.\n\nDas Hotel steht für absolute Superlative in allen Bereichen und dem Gast soll hier nur das Beste geboten werden. Man kann hier Zimmer von mit 58 m² bis zu Suiten mit 650 m² buchen. Man kann hier einen Ferrari oder Maserati aus dem hauseigenen Fuhrpark für einen Ausflug mieten. Man kann auch den Golfplatz mit 18 Löchern nutzen oder sich von den dort arbeitenden Spitzenköchen verwöhnen lassen.\n\nIm Hotel befindet sich die imposante Kunstsammlung des Erbauers und man kann Theaterproduktionen, eine Artistik Show oder die  Wasserrevue Le Rêve besuchen. Den Abend kann man auch in diversen exklusiven Nachtclubs ausklingen lassen.\n\nBei unserem Rundgang durch das Hotel sind wir erst durch eine exklusive Einkaufspassage mit Geschäften von Chanel und Louis Vuitton gelaufen und sind dann an Bars, tollen Restaurants, Hochzeitskapellen und einem gigantisch großen Casino mit 1.800 Spielautomaten und 26 Pokertischen vorbeigekommen. Alles wirkt hier sehr edel und das Publikum hier im Hotel ist sehr gehoben.\n\nEin wirklich tolles Hotel das in Las Vegas seinesgleichen sucht.'), Row(stars=5.0, useful=207, text='Auf unserer Rundreise haben wir häufig die Restaurantkette In-n-Out Burger besucht, die mit dem Versprechen "Quality You Can Taste" wirbt. In Las Vegas , an der Sahara Avenue, sind wir eingekehrt, da die Filiale am Strip total überlaufen ist. Im Internet konnte ich nachlesen, dass es das Unternehmen bereits seit 1948 gibt und es mittlerweile ca. 300 Restaurants im Westen der USA und Texas gibt.\n\nAm späten Abend war hier der Andrang nur mittelmäßig. Die Auswahl war einfach, da es nur drei verschiedene Burger und Pommes zur Auswahl gibt. Wenn man es dann zur Bestellung geschafft hat, bekommt man eine Nummer, die dann aufgerufen wird sobald die Bestellung fertig ist.\n\nAuf die Burger kommt nur gekühltes und kein gefrorenes Frischfleisch, das von der eigenen Fleischfabrik innerhalb von 24 Stunden geliefert wird. Die Kartoffeln für die Pommes werden im Restaurant aus frischen Kartoffeln hergestellt. Ich konnte es selber in der offenen Küche sehen, wie ein Mitarbeiter die Kartoffeln geschält und gewürfelt hat.\n\nPreislich bewegt man sich trotzdem weit unten, da man auf eine umfangreiche Auswahl verzichtet. Wenn man mit mehreren Leuten hier einkehrt, sollten die anderen ein Plätzchen im Restaurant sichern, da diese zu den Stoßzeiten ebenfalls rar sind.\n\nVor den meisten Läden findet man zwei gepflanzte Palmen, die gekreuzt wachsen und eine Anspielung auf den Lieblingsfilm des Inhabers "Eine total, total verrückte Welt" sein sollen. Eine weitere Marotte sind angedruckte Bibelstellen auf den Unterseiten der Getränkebecher.\n\nVon der Qualität des Essens und dem Preis- Leistungsverhältnis waren wir wirklich begeistert. Unser Lieblingsrestaurant während unserer Rundreise.'), Row(stars=5.0, useful=203, text='Auf unserer Casino Besichtigungstour sind wir auch auf den noch recht neuen Park "The Park" gestoßen, der sich neben der T-Mobile Arena, dem Monte Carlo Hotel und dem New York-New York Resort befindet. Die vielen schattigen Sitzmöglichkeiten haben uns zu einer kleinen Pause eingeladen.\n\nIm April 2016 wurde dieser erste öffentliche Park von Las Vegas eröffnet und ist aus meiner Sicht wirklich gelungen. So hat man hier ca. 250 für diesen Ort geeignet Bäume gepflanzt und weitere 7.000 Pflanzen. Ergänzt wird das Konzept des Landschaftsarchitekten !melk mit einigen Wasserinstallationen.\n\nHerzstück des Parks ist sicherlich die 40 Fuß hohe Skulptur "Bliss Dance" und die 18 Meter hohen Baumkonstruktionen aus Stahl. Diese spenden den Besuchern Schatten und leuchten in der Nacht wunderschön. Die aufwendigen Konstruktionen wurden von einem Schiffsbauer in den Niederlanden gebaut. \n\nDer Park schlängelt sich auf der rechten Seite des New York-New York Resort entlang und bietet den Besuchern einige Lokalitäten mit frischen Drinks und Essen. So gibt es hier Beerhaus, Bruxie, SakeRok und California Pizza Kitchen. Im Eingangsbereichs des New York-New York Resort kann man auch Burger von der angesagten Restaurantkette Shake Shack käuflich erwerben.\n\nWir hatten uns aber lediglich ein schattiges Plätzchen gesucht und das bunte Treiben um uns herum beobachtet. Dieser Park ist wie in den USA üblich top gepflegt und sauber. Hier kann man sehr gut von der schrillen Casinostadt abschalten. Ein wirklich toll angelegter Park.')]
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

Using the checkin data file, we can return an easily readable view of the DataFrame schema using the .printSchema() function to find the hour of the day in which most check-ins occur. We'll start with the two column names, "business_id" and "date", to find the information we need.

```python
checkin.printSchema()

root
 |-- business_id: string (nullable = true)
 |-- date: string (nullable = true)

```

Next, we'll extract all dates from when someone checked in by using the .split() function on comma with the result of a list of strings.

```python
datesplit = udf(lambda x: x.split(','),ArrayType(StringType()))
```

Using .select(), we'll return the "business_id" column, apply the datesplit function to the "date" column, then create a new .alias() column labeled "dates".

```python
df_hour=checkin.select('business_id',datesplit('date').alias('dates'))
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

To find the hour of the day with the most check-ins, the hour function can be used to extract the hours of a given date as an integer. The data needs to be trimmed of empty spaces so we'll use the trim function to trim the spaces from both ends for the string column. Then creating another alias with a column labeled "finaldates", we'll use .groupBy(), .count(), then .sort() to return a list of hours and their count in descending order to get the result we're looking for.

```python
df_hour2=df_hour_exploded.select(trim(df_hour_exploded['checkin_date']).alias('finaldates')).groupBy(hour('finaldates').alias('final')).count().sort("count",ascending=False).show(24)

+-----+-------+
|final|  count|
+-----+-------+
|    1|1561788|
|   19|1502271|
|    0|1491176|
|    2|1411255|
|   20|1350195|
|   23|1344117|
|   18|1272108|
|   22|1257437|
|   21|1238808|
|    3|1078939|
|   17|1006102|
|   16| 852076|
|    4| 747453|
|   15| 617830|
|    5| 485129|
|   14| 418340|
|    6| 321764|
|   13| 270145|
|    7| 231417|
|   12| 178910|
|    8| 151065|
|   11| 111769|
|    9| 100568|
|   10|  88486|
+-----+-------+

```

## Example 4 - Sentiment Analysis

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

Pre-processing the reviews data

Let's find the most common words that are unique to positive and negative reviews. First, we need to clean the text of the reviews by removing any punctuation or numbers using the following function.

```python
# remove punctuation
def remove_punct(text):
    regex = re.compile('[' + re.escape(string.punctuation) + '0-9\\r\\t\\n]')
    nopunct = regex.sub("", text) 
    return nopunct
```

The "stars" column must be relabelled so that any reviews with 4 stars or above will be positive, anything else is considered to be negative.
This benchmark is based on a general consensus about Yelp reviews where people tend to overrate restaurants/businesses unless they feel strongly negatively about the place. We also remove punctuations and numbers before tokenizing the text.
Reference

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

Since we're looking for the top words from positive and negative reviews, creating two separate tables will be the best way to find unique words. Having labeled all positive reviews 1 and all negative reviews as 0, we can create a filter with a condition to pull rows from rows labeled as such.

```python
pos=dfwords_exploded.filter(dfwords_exploded['label'] >= 1)
neg=dfwords_exploded.filter(dfwords_exploded['label'] < 1)
```

Using .select(), .groupBy(), .count(), then .sort(), we can organize the most commonly used words in positive and negative reviews. This information isn't quite helpful since there is a lot of commonality between the tables. We are interested in unique words so we'll try a join technique next.

```python
posdf=pos.select('words','label').groupBy("words",'label').count().sort("count",ascending=False)
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
negdf=neg.select('words','label').groupBy("words",'label').count().sort("count",ascending=False)
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
ta = posdf.alias('ta')
tb = negdf.alias('tb')
```

An inner join is the default join type used. This joins two datasets on key columns, where keys don't match the rows get dropped from both datasets.
Reference

```python
inner_join = ta.join(tb, ta.words == tb.words)
inner_join.show()

+-------------+-----+-----+-------------+-----+-----+
|        words|label|count|        words|label|count|
+-------------+-----+-----+-------------+-----+-----+
|        aaagh|    1|    3|        aaagh|    0|    2|
|     abilityi|    1|    8|     abilityi|    0|    4|
|  aboutdollar|    1|    1|  aboutdollar|    0|    1|
| aboutfirstly|    1|    2| aboutfirstly|    0|    1|
|   abouttoday|    1|    1|   abouttoday|    0|    3|
|        abrio|    1|    1|        abrio|    0|    1|
|   abruptness|    1|   16|   abruptness|    0|   29|
|accelerationi|    1|    1|accelerationi|    0|    1|
|      accthis|    1|    1|      accthis|    0|    1|
| accumulation|    1|   88| accumulation|    0|   73|
|accuratelybut|    1|    1|accuratelybut|    0|    1|
|   achterbahn|    1|    8|   achterbahn|    0|    2|
|      acidity|    1| 1899|      acidity|    0|  192|
|   activewear|    1|   52|   activewear|    0|    3|
| adequatethis|    1|    4| adequatethis|    0|    1|
|      admitso|    1|    1|      admitso|    0|    2|
|        adour|    1|    1|        adour|    0|    1|
|   advancedwe|    1|    2|   advancedwe|    0|    1|
|     aeroport|    1|   18|     aeroport|    0|   18|
|     aerostar|    1|    1|     aerostar|    0|    1|
+-------------+-----+-----+-------------+-----+-----+
only showing top 20 rows
```

Using a leftanti join returns only columns from the left dataset for non-matched words. This is how we'll return the top unique words from the tables.

```python
uniquepos =ta.join(inner_join, on='words', how='leftanti').sort("ta.count",ascending=False).show(50)
uniqueneg =tb.join(inner_join, on='words', how='leftanti').sort("tb.count",ascending=False).show(50)

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
{"mode":"full","isActive":false}
{"mode":"full","isActive":false}
