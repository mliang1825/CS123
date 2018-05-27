from scipy.sparse import lil_matrix, coo_matrix, bmat
from scipy.io import mmwrite
from mrjob.job import MRJob
import pandas as pd
from mrjob.step import MRStep
import os
import csv

class GetCount(MRJob):
    def init_get_words(self):
        '''
        Creates the dictionary of words to reference and the dictionary length
        Reads the output file from the word processing script
        '''
        df = pd.read_csv("~/output.txt", delimiter="\t", header=None)
        self.word_dict = dict(zip(df[0], df[1]))
        self.num_words = len(self.word_dict)

    def mapper(self, key, line):
        '''
        Maps a json file with Amazon review data to a dictionary that contains
        all words in that review that are also in the word processing output
        Yields:
        Number of words in the output, 
        (reviewerID and productid dictionary, dictionary of review word counts)
        '''
        l = eval(line)
        reviewerID = l["reviewerID"]
        asin = l["asin"]
        review = l["reviewText"].lower().split(" ")
        ref_dict = {"reviewerID": reviewerID, "asin": asin}
        #sparse_block = lil_matrix((1,len(self.word_dict)))
        current_dict = {}
        for word in review:
            if word in self.word_dict:
                if str(self.word_dict[word]) in current_dict:
                    current_dict[str(self.word_dict[word])] += 1
                else:
                    current_dict[str(self.word_dict[word])] = 1
        yield self.num_words, (ref_dict, current_dict)

    def reducer_file(self, key, value):
        '''
        Reduces the dictionary of word counts to a sparse big_matrix
        Outputs:
        words_matrix.mtx: Sparse matrix, rows = reviews, columns = word counts
        reviews_index.csv: Maps indices in csv file to reviewerID and asin
        '''
        dict_list = list(value)
        ref_list = []
        big_matrix = lil_matrix((len(dict_list), key + 1))
        for i in range(len(dict_list)):
            for x in dict_list[i][1]:
                big_matrix[i, x] = dict_list[i][1][x]
            ref_list.append((i, dict_list[i][0]["asin"], dict_list[i][0]["reviewerID"]))
        #output = "\n".join(ref_list)
        #print(os.getcwd())
        with open("/home/jonpekarek/reviews_index.csv", "w+") as f:
            writer = csv.writer(f)
            writer.writerows(ref_list)
        mmwrite("/home/jonpekarek/words_matrix.mtx", big_matrix, field = "integer")

    def steps(self):
        return [MRStep(mapper_init=self.init_get_words,
                       mapper=self.mapper,
                       reducer=self.reducer_file)]


if __name__ == '__main__':
    GetCount.run()
