from mrjob.job import MRJob

class FilterAllBelowAvg(MRJob):
    '''
    This file is a precursor for threshold_count.py. No longer used. The file takes the output
    of pairings.py and counts all the pairs that are below the average. The average
    is specified in a avg.txt file put in with the argument --threshold.
    '''

    def configure_options(self):
        super(FilterAllBelowAvg, self).configure_options()
        self.add_file_option('--threshold')

    def mapper(self, _, line):
        entries = line.split()
        index1 = int(entries[0].strip("[]").strip(","))
        index2 = int(entries[1].strip("]"))
        val = float(entries[2])
        yield (index1,index2), val

    def reducer_init(self):
        with open(self.options.threshold, 'r') as f:
            first_line = f.readline().split()
        self.threshold = float(first_line[1])


    def reducer(self, indexes, val):
        val = list(val)[0]
        if val < (self.threshold / 2):
            yield indexes, val

if __name__ == '__main__':
    FilterAllBelowAvg.run()
