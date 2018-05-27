from mrjob.job import MRJob
from mrjob.step import MRStep

class GetPercentBelowThreshold(MRJob):
    '''
    In the 150kline_pairs file on our cloud storage, takes in two 
    additional text files: the threshold, which contains the average 
    of the distances as the second item on the first line of the file, 
    and the subtraction, which contains only the number we 
    want to subtract from the threshold to obtain the threshold for this 
    experiment. This program finds the percentage of entries in the 
    150kline_pairs file below the true threshold and prints that percentage
    to the output file (if one is given in the command line argument.)
    '''
    def configure_options(self):
        super(GetPercentBelowThreshold, self).configure_options()
        self.add_file_option('--threshold')
        self.add_file_option('--subtraction')


    def mapper_get_counts(self, _, line):
        entries = line.split()
        index1 = int(entries[0].strip("[]").strip(","))
        index2 = int(entries[1].strip("]"))
        val = float(entries[2])
        yield (index1,index2), val


    def reducer_get_counts_init(self):
        with open(self.options.threshold, 'r') as f:
            first_line = f.readline().split()
        self.threshold = float(first_line[1])
        with open(self.options.subtraction, 'r') as s:
            number = s.readline().split()[0]
        self.subtraction = float(number)


    def reducer_get_counts(self, indexes, val):
        val = list(val)[0]
        if val < (self.threshold - self.subtraction):
            yield None, 1
        else: 
            yield None, 0


    def reducer_merge_counts(self, _, nums):
        summ = 0
        count = 0
        for num in nums:
            summ += num
            count += 1
        yield None, summ/count


    def steps(self):
        return [
        MRStep(mapper=self.mapper_get_counts,
            reducer_init=self.reducer_get_counts_init,            
             reducer=self.reducer_get_counts),
        MRStep(reducer=self.reducer_merge_counts)
        ]


if __name__ == '__main__':
    GetPercentBelowThreshold.run()