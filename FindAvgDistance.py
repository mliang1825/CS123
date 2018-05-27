from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgUniqVal(MRJob):
    '''
    Obtains the average unique value of the distances between the rows in the
    matrix of review vocabularies, e.g. rows 100 and 200 might have a score of
    20. 
    '''
    def mapper_find_vals(self, _, line):
        entries = line.split()
        yield entries[2], None
  

    def combiner_find_vals(self, val, _):
        yield val, None
  

    def reducer_find_vals(self, val, _):
        yield None, val


    def reducer_average_val(self, _, vals):
        summ = 0
        count = 0
        for val in vals:
            summ += val
            count += 1
        yield None, summ/count


    def steps(self):
        return [
        MRStep(mapper=self.mapper_find_vals,
             combiner=self.combiner_find_vals,
             reducer=self.reducer_find_vals),
        MRStep(reducer=self.reducer_average_val)
        ]


if __name__ == '__main__':
  AvgUniqVal.run()