from mrjob.job import MRJob
from mrjob.step import MRStep

'''
The purpose of this MapReduce code is to count the number of pair reviews that
were for the same product or came from the same person. 'asin' refers to the
product ID and reviewerID refers to the reviewerid. We express these numbers
for vector distances below various thresholds (1 - 23). Note that the average
vector distance is 22.9. The file takes an extra file argument, --index.
The index file is a three column csv in the format (row, asin/product id, reviewerid).
'''

THRESHOLDS = [n for n in range(1,24)]


class MRFindMatches(MRJob):

    def configure_options(self):
        super(MRFindMatches, self).configure_options()
        self.add_file_option('--index')

    def mapper(self, _, line):
        #Yields the index pairs and thresholds with
        #val below the thresholds.
        entries = line.split()
        index1 = int(entries[0].strip("[]").strip(","))
        index2 = int(entries[1].strip("]"))
        val = float(entries[2])
        for threshold in THRESHOLDS:
            if val < threshold:
                yield (index1,index2, threshold), val

    def reducer_init(self):
        self.ids_list = []
        with open(self.options.index) as f:
            for line in f:
                self.ids_list.append(line.strip().split(','))


    def reducer(self, pairs, val):
        #Counting all the same asin and reviewerid.
        indexA = pairs[0]
        indexB = pairs[1]
        threshold = pairs[2]
        asinA = self.ids_list[indexA][1]
        asinB = self.ids_list[indexB][1]
        reviewerIDA = self.ids_list[indexA][2]
        reviewerIDB = self.ids_list[indexB][2]
        if asinA == asinB:
            yield ('asin', threshold), 1

        if reviewerIDA == reviewerIDB:
            yield ('reviewerid', threshold), 1

    def reducer_sum(self, category, counts):
        yield category, sum(counts)

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer_init = self.reducer_init,
                   reducer = self.reducer),
            MRStep(reducer = self.reducer_sum)
        ]

if __name__ == '__main__':
    MRFindMatches.run()
