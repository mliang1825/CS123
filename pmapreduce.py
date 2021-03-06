from scipy.sparse import lil_matrix
from scipy.io import mmwrite
from mrjob.job import MRJob
import pandas as pd
from mrjob.step import MRStep
import csv

class GetCount(MRJob):
    '''
    Takes a json file with amazon review data and uses the word processing
    output to create a sparse matrix with counts of each word in each review
    and a file mapping the rows in the sparse matrix to the review data
    '''
    def configure_options(self):
        '''
        Three command line options, which are the word processing results and 
        the two desired output filenames
        --words: the output from the word processing task
        --index: the output filename for the row index to review mapping file
        --matrix: the output filename for the sparse matrix
        '''
        super(GetCount, self).configure_options()
        self.add_passthrough_option(
        '--words', type='str', default="output.txt")
        self.add_passthrough_option(
        '--index', type='str', default="reviews_index.csv")
        self.add_passthrough_option(
        '--matrix', type='str', default="words_matrix.mtx")

    def init_get_words(self):
        '''
        Creates the dictionary of words to reference and the dictionary length
        Reads the output file from the word processing script
        '''
        df = pd.read_csv(self.options.words, delimiter="\t", header=None)
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
        with open(self.options.index, "w+") as f:
            writer = csv.writer(f)
            writer.writerows(ref_list)
        mmwrite(self.options.matrix, big_matrix, field = "integer")

    def steps(self):
        return [MRStep(mapper_init=self.init_get_words,
                       mapper=self.mapper,
                       reducer=self.reducer_file)]


if __name__ == '__main__':
    GetCount.run()
