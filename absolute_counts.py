from mrjob.job import MRJob
from mrjob.step import MRStep

'''
The purpose of this MapReduce code is to count the number of pair reviews that
fall beneath each threshold. Naturally, as the threshold decreases,
the count will decrease.
'''

THRESHOLDS = [n for n in range(1,24)]


class MRFindCounts(MRJob):

    def mapper(self, _, line):
        #Yield a count for each pair.
        entries = line.split()
        val = float(entries[2])
        for threshold in THRESHOLDS:
            if val < threshold:
                yield threshold, 1

    def combiner(self, threshold, counts):
        yield threshold, sum(counts)

    def reducer(self, threshold, count):
        #Counting all thresholds.
        yield threshold, sum(count)




if __name__ == '__main__':
    MRFindCounts.run()
