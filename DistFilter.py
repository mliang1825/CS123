from mrjob.job import MRJob 

class FilterAllBelowAvg(MRJob):

    def configure_options(self):
        super(FilterAllBelowAvg, self).configure_options()
        self.add_file_option('--threshold')

    def mapper(self, _, line):
        entries = line.split()
        indexes = entries[0] + " " + entries[1]
        val = float(entries[2])
        return indexes, val

    def reducer_init(self):
        with open(self.options.threshold) as f:
            line = f[0]
            self.threshold = float(line[1])


    def reducer(self, indexes, val):
        if val < (self.threshold / 2):
            yield indexes, val

if __name__ == '__main__':
    FilterAllBelowAvg.run()