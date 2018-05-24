from mrjob.job import MRJob
import pandas as pd
from mrjob.step import MRStep


class GetCount(MRJob):
    def init_get_words(self):
        df = pd.read_csv("output.txt", delimiter="\t", header=None)
        self.word_dict = dict(zip(df[0], df[1]))

    def mapper(self, key, line):
        l = eval(line)
        reviewerID = l["reviewerID"]
        asin = l["asin"]
        review = l["reviewText"].lower().split(" ")
        current_dict = {"reviewerID": reviewerID, "asin": asin}
        for word in review:
            if word in self.word_dict:
                if self.word_dict[word] in current_dict:
                    current_dict[self.word_dict[word]] += 1
                else:
                    current_dict[self.word_dict[word]] = 1
        yield None, str(current_dict)

    def reducer_file(self, _, value):
        output = "\n".join(list(value))
        with open("counted_words.txt", "w+") as f:
            f.write(output)

    def steps(self):
        return [MRStep(mapper_init=self.init_get_words,
                       mapper=self.mapper,
                       reducer=self.reducer_file)]


if __name__ == '__main__':
    GetCount.run()
