from mrjob.job import MRJob
from mrjob.step import MRStep
#import numpy as np
import math

#Remeber to use tail -n +4 bmatrixtest.mtx > bmatrixtest2.mtx to get rid of first 3 lines of the matrix.
ROWS = 1000
COLUMNS = 1000
#Rows and columns of the matrix must be know beforehand. Example:
#words_matrix.mtx Rows = 13272. Columns = 60342. Points = 793344.

def norm(tuple_list):
    '''
    In the instance that we have a comparison where one of the rows
    is all 0, then we need only find the norm of the other row.
    This function takes care of that.
    '''
    squared_distance = 0
    for pair in tuple_list:
        squared_distance += (pair[1] ** 2)

    return math.sqrt(squared_distance)


class MRStepSix(MRJob):

    def mapper(self, key, line):
        line = line.split(' ')
        row = int(line[0]) - 1
        col = int(line[1]) - 1
        count = int(line[2])

        yield row, (col,count)

    def reducer(self, row, pair):
        col_value_pairs = list(pair)
        for n in range(0, row):
            yield (n,row) , col_value_pairs
        for n in range(row + 1, ROWS):
            yield (row, n) , col_value_pairs

    def reducer2(self, row, pair):
        col_value_pairs = list(pair)
        for n in range(0, ROWS):
            if n < row:
                yield (row, n), col_value_pairs
            if n > row:
                yield (n,row), col_value_pairs

    def reduce_calc(self, row_pair, vectors):
        lists = list(vectors)

        if len(lists) == 2:
            if len(lists[0]) <= len(lists[1]):
                short_list = sorted(lists[0], key = lambda x: x[0])
                long_list = sorted(lists[1], key = lambda x: x[0])
            else:
                short_list = sorted(lists[1], key = lambda x: x[0])
                long_list = sorted(lists[0], key = lambda x: x[0])

            long_index = 0
            #short_index = 0
            len_l = len(long_list)
            squared_distance = 0

            for short_index in range(0,len(short_list)):
                if long_index == len_l:
                    value_s = short_list[short_index][1]
                    squared_distance += (value_s ** 2)
                    continue

                short_column = short_list[short_index][0]
                long_column = long_list[long_index][0]

                if short_column < long_column:
                    value_s = short_list[short_index][1]
                    squared_distance += (value_s ** 2)

                elif short_column == long_column:
                    value_s = short_list[short_index][1]
                    value_l = long_list[long_index][1]
                    s_distance = ((value_s - value_l) ** 2)
                    squared_distance += s_distance
                    long_index += 1

                else:
                    #We will move long index until short_column < long_column again.
                    while long_list[long_index][0] < short_column:
                        value_l = long_list[long_index][1]
                        squared_distance += (value_l ** 2)
                        long_index += 1
                        if long_index == len_l:
                            break

                    value_s = short_list[short_index][1]
                    squared_distance += (value_s ** 2)

            for n in range(long_index, len(long_list)):
                value_l = long_list[n][1]
                squared_distance += (value_l ** 2)

            distance = math.sqrt(squared_distance)
            yield row_pair, distance

        else:
            pairs = lists[0]
            distance = norm(pairs)

            yield row_pair , distance

    def sanity(self, _, value):
        yield None, sum(value)

    def steps(self):
        return [
        MRStep(mapper = self.mapper,
            reducer = self.reducer2),
        MRStep(reducer = self.reduce_calc)
        #MRStep(reducer = self.sanity)
    ]

if __name__ == '__main__':
    MRStepSix.run()
