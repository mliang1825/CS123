## CS123 Amazon Reviews Project
# Josef Klafka, Timmy Li, Michelle Liang, Jon Pekarek
This is the Github repository for our Computer Science with Applications - III final project, in which we analyzed a large dataset of Amazon reviews to uncover similarities between reviews, with the goal of uncovering fake reviews or companies paying reviewers to leave large volumes of reviews. 

# Python files - description: 
pmapreduce.py - yields the counts of all words for each review in a sparse matrix, and a file that maps reviewerid and productid to row indices in that sparse matrix

Pairings.py- Takes sparse matrix file, pairs up all rows, and calculates the distance between word vectors.

Threshold_counts.py - Takes the output of pairings.py, for a variety of thresholds, calculated the number of matching product ids and the number of matching reviewer ids for the pairs with distances below that threshold.

GetTop40Words.py - Takes an Amazon reviews JSON file and filters to find the top 40 unique words. 

FindAllButTop40.py - Takes an Amazon reviews JSON file and the output of GetTop40Words.py, and filters to return all unique words which are not in the top 40 words from GetTop40Words.py. 

FindAvgDistance.py - Given the output of Pairings.py, found the average distances between rows in the sparse matrix. 

GetPercentBelowThreshold.py - Given the output of Pairings.py, and the threshold average from FindAvgDistance.py along with a number to subtract from the average, returned the percentage of reviews below the threshold minus the subtraction. 
