# Homework 2
### Description: Implementation of MapReduce Jobs to perform the various tasks on [dblp](https://dblp.uni-trier.de/xml/) dataset and deploying it on AWS EMR and S3 instances.

## Link Demonstrating deployment  and running of job on AWS EMR - [a link](https://www.youtube.com/watch?v=Bf9jDGrR2IQ&lc=UgwukfyrfNnX-BotE094AaABAg)

## Name - Gautamkumar Ojha 

## Steps to Set-up Environment and run the jobs.

1. Cloning the Project -
 
```git clone https://ojhagautam97@bitbucket.org/cs441-fall2020/gautamkumar_ojha_hw2.git```


2. Navigate to cd gautamkumar_ojha_hw2\code and clean, compile the code and build the jar which will be generated at gautamkumar_ojha_hw2\code\target\scala-2.13\code-assembly-0.1.jar:

```sbt clean compile assembly```

3. I have used HDP Sandbox on VMWare so to transfer files I use WinScp from my Windows to the HDP Sandbox. Thus we copy the jar and dblp.xml input from local machine to HDP sandbox.

4. Now we connect to the sandbox as root :

```ssh root@sandbox-hdp.hortonworks.com -p 2222```

5. Making Input directory as tmp to store dblp.xml as:

```hdfs dfs -mkdir /tmp```

6. Putting dblp.xml in dir tmp as:

```hdfs dfs -put dblp.xml /tmp/```

7. Now the final step is just ls -lrt to check whether dblp dataset has been transferred properly and have root access, then finally execute the jar as:

```hadoop jar code-assembly-0.1.jar code /tmp/dblp.xml /tmp/op```

Note - You can delete the op file and re run other jobs in it or simply just change the output file name and run all the program.

## Map - Reduce Tasks Performed

* Tags Used for venues -

```
For article tag - <journal>
For inproceedings/proceedings/incollection tag - <booktitle>
For book/phdthesis/msthesis - <publisher>
```

* Tags Used for authors -

```
<authors> in all cases but <editor> when <author> tag is absent.
```

* Tags Used for Publications - 

```
<title>
```

## XMLINPUTFORMAT

The Implementation to successfully take xml as input for the code is used from [Mahout's Implementation](https://github.com/apache/crunch/blob/master/crunch-core/src/main/java/org/apache/crunch/io/text/xml/XmlInputFormat.java) and 
[Ajith's Implementation](https://github.com/ajithnair20/DBLPMapReduce/blob/master/src/main/java/mapreduceinputformat/XmlInputFormatWithMultipleTags.java).



#### Task 1 : Top Ten Published Author at Each Venue.

1. This task is done by implementing one mapper and one reducer class under package TopTenAuthorsAtEachVenue. The mapper takes list of authors in the venue and emits 
<Venue, Author> to the Reducer.

2. The Reducer receives input from mapper and computes the list of author for that venue as authors_list, then the authors_list is used to compute another list of topTenAuthors as 
demonstrated in the code.

3. Finally, the Reducer emits <Venue, List of Top Ten Authors> as output.

Sample Output for Task 1 is : <Venue, List of Top Ten Authors separated by ;> 

```
Op1 - 35 years of fuzzy set theory,bart van gasse;magda komorníková;miguel pagola;irina perfilieva;mike nachtegael;yun shi;martine de cock;vilém novák;yassine djouadi;xia wu
Op2 - 3d integration for noc-based soc architectures,paul d. franzon;narayanan vijaykrishnan;eby g. friedman;yuan xie 0001;tadahiro kuroda;frédéric pétrot;shan yan;dinesh pamunuwa;bill lin;vasilis f. pavlidis
Op3 - 3d research challenges in cultural heritage,dieter w. fellner;sander münster;sven havemann;florian rist;nicolas billen;matteo dellepiane;daniel dworak;wolfgang hegel;moritz neumüller;marc grellert
```


#### Task 2 : Produce List of Publications for each Venue that contains only one Author.

1. This task is done by implementing one mapper and one reducer class under package PubsWithOneAuthorForEachVenue. The Mapper emits <Venue, Publisher> by checking if the authorlist is of size 1 
for the venue and publications list it has computed.

2. The Reducer receives input from mapper and computes list of publications for each venue by combining and reducing input received as shown in code.
It emits <Venue, List of Publications with one author>  as output separated by -> 

Sample Output for Task 2 is : <Venue, List of Publications with one author separated by ->  >

```
Op1 - 3d flash memories,the business of nand.->3d flash memories->3d vg-type nand flash memories.
Op2 - 3d image processing, measurement (3dipm), and applications,small scale surface profile recovery using a tunable lens based system.->a 3d mesh quality metric based on features fusion.
Op3 - 3d imaging, analysis and applications,representing, storing and visualizing 3d data.->high-resolution three-dimensional remote sensing for forest measurement.
```

#### Task 3 : Produce List of Publications for each Venue that contains highest number of Authors.

Note - This Task gave the most because of bad input type of XML few of them I could handle and produced output for a smaller dataset.

1. This task is done by implementing one mapper and one reducer class under package PubsWithMostAuthorForEachVenue.

2. The Mapper emits <Venue, (Publication name, Author Count)> as done in the code. The value is Passed as Text to the Reducer.

3. The Reducer here receives values from Mapper and implements a tree map. The tree map has keys as author count and publication name as value for each venue.
I have tried handling bad input type using regex which is assigned to val pattern and also in Mapper while emitting the values.

4. For Each tuple value I am splitting the tuple into a int value and string value where int is author count and string is title. I am then checking
whether the tree map already has that key then I am just appending the title for that count eg :- c1 -> (t1,t2), otherwise I am just putting key and value in the tree map.

5. Finally, my reducer emits Venue as key and the first value of reverse ordered tree map as value. <Venue, new Text(tree_map(tree_map.firstKey)>

Sample Output for Task 3 :  <Venue, List of Publications by highest authors are separated by |>

```
Op1 - a construction manual for robots' ethical systems,the potential of logic programming as a computational tool to model morality.|constrained incrementalist moral decision making for a biologically inspired cognitive architecture.|ethical regulation of robots must be embedded in their operating systems.|case-supported principle-based behavior paradigm.|shall i show you some other shirts too? the psychology and ethics of persuasive robots.|grafting norms onto the bdi agent model.
Op2 - advanced topics in computer vision,learning object detectors in stationary environments.|co-recognition of images and videos: unsupervised matching of identical object patterns and its applications.|moment constraints in convex optimization for segmentation and tracking.|evaluating and extending trajectory features for activity recognition.|boosting k-nearest neighbors classification.|recognizing human actions by using effective codebooks and tracking.|large scale metric learning for distance-based image classification on open ended data sets.|video temporal super-resolution based on self-similarity.
```

#### Task  : Produce List of 100 Authors in Desc Order with Most Co-Author and without any Co-Author.

1. Both sub tasks were done similarly by implementing a mapper and reducer for each sub tasks and sorted using two class(DescendingSortMapper, DescendingSortReducer) and a custom comparator(SortComparator) under the package SortListOfAuthors.

2. For 1st sub task the Mapper emits authors and it's co-authors to the reducer. The Reducer just receives the output from mapper and emits author with it's count to the second job for swapping and sorting.

3. For 2nd sub task the Mapper emits author and one as it's values for author list of size 1. The Reducer recieves the output from maapper and emits author and it's summed/combied up count to the second job for swapping and sorting.

4. Finally, I have implemented the second job with one mapper(DescendingSortMapper) which swaps the key value pair received from job1 and one reducer(DescendingSortReducer) which just emits the swapped key value pairs till the count is 100. 
Lastly, the custom comparator(SortComparator) class sorts in descending order based on key as Int.

Sample Output for Task 4a : <Author, Co-Author Count>

```
h. vincent poor,5486
wei li,5031
wen gao 0001,4840
lei zhang,4810
wei zhang,4720
yu zhang,4705
yang liu,4664
philip s. yu,4643
nassir navab,4482
```

Sample Output for Task 4b : <Author, Count>


```
t. d. wilson 0001,348
ronald r. yager,346
robert l. glass,262
diane crawford,250
peter g. neumann,240
david alan grier,219
elena maceviciute,218
harold joseph highland,214
karl rihaczek,195
louis kruh,194
```


###Output Files

I have the Output files under the output folder in code folder. I would say you should preferably open the non csv file in any
text editor(I read in VS code) because it's easy interpretable and understandable.

###Testing 

I have done local testing in terminal using command:

```
sbt clean compile test
```

























