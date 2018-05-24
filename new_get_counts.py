from scipy.sparse import lil_matrix, coo_matrix, bmat
from scipy.io import mmwrite
from mrjob.job import MRJob
import pandas as pd
from mrjob.step import MRStep

class GetCount(MRJob):
    def init_get_words(self):
        df = pd.read_csv("/home/student/output.txt", delimiter="\t", header=None)
        self.word_dict = dict(zip(df[0], df[1]))
        self.num_words = len(self.word_dict)

    def mapper(self, key, line):
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
        dict_list = list(value)
        ref_list = []
        big_matrix = lil_matrix((len(dict_list), key))
        for i in range(len(dict_list)):
            for x in dict_list[i][1]:
                big_matrix[i, x] = dict_list[i][1][x]
            ref_list.append(str((i+1, dict_list[i][0])))
        output = "\n".join(ref_list)
        with open("/home/student/reviews_index.txt", "w+") as f:
            f.write(output)
        mmwrite("/home/student/words_matrix.mtx", big_matrix, field = "integer")

    def steps(self):
        return [MRStep(mapper_init=self.init_get_words,
                       mapper=self.mapper,
                       reducer=self.reducer_file)]


if __name__ == '__main__':
    GetCount.run()
