# CS123 Amazon Reviews Project
### Josef Klafka, Timmy Li, Michelle Liang, Jon Pekarek
This is the Github repository for our Computer Science with Applications - III final project, in which we analyzed a large dataset of Amazon reviews to uncover similarities between reviews, with the goal of uncovering fake reviews or companies paying reviewers to leave large volumes of reviews.

### Python files - description:
pmapreduce.py - yields the counts of all words for each review in a sparse matrix, and a file that maps reviewerid and productid to row indices in that sparse matrix

pairing.py- Takes sparse matrix file, pairs up all rows, and calculates the distance between word vectors.

threshold_count.py - Takes the output of pairings.py, for a variety of thresholds, calculated the number of matching product ids and the number of matching reviewer ids for the pairs with distances below that threshold.

GetTop40Words.py - Takes an Amazon reviews JSON file and filters to find the top 40 unique words.

FindAllButTop40.py - Takes an Amazon reviews JSON file and the output of GetTop40Words.py, and filters to return all unique words which are not in the top 40 words from GetTop40Words.py.

FindAvgDistance.py - Given the output of Pairings.py, found the average distances between rows in the sparse matrix.

GetPercentBelowThreshold.py - Given the output of Pairings.py, and the threshold average from FindAvgDistance.py along with a number to subtract from the average, returned the percentage of reviews below the threshold minus the subtraction.

DistFilter2.py - No longer used. Precursor to threshold_counts.py. Counts the number of pairs below a threshold.

visuals_count.py - Creates visualizations for counts. (Created counts_viz.html)

visuals_percents.py - Creates visualizations for percents. (created percents_viz.html)

### Other files - description
CS 123, Proposal.pdf - Project Proposal

avg.txt - Contains the average distance between the rows in the words_matrix

counts_viz.html - Visualizations in counts.

matchings.csv - csv file for the counts of matching productid/reviewerid below each threshold.

vocab.txt - 60000-word vocabulary used as the basis for the columns in the matrix

percents_viz.html - Visualizationo in percents.

reviews_index.csv - Index file indicating the row within the matrix, the product id, and the reviewerid

words_matrix.mtx - a test file.

total_thresh_counts.csv - csv file for the total counts below each threshold.
