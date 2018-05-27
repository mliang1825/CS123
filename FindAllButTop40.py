from mrjob.job import MRJob 
import json
import re

class FindAllButTop40(MRJob):
	'''
	Given a JSON file of Amazon review data, and the top 40 words in the
	JSON file as an auxiliary file, filters the JSON file to return the unique
	words outside of the top 40 words. 
	'''
    def configure_options(self):
        super(FindAllButTop40, self).configure_options()
        self.add_file_option('--database') #for the top40 words file


    def mapper(self, _, line):
        review = json.loads(line)
        words = re.findall(r'[a-zA-Z]+', review['reviewText'])
        for word in words:
            yield word.lower(), 1


    def combiner(self, word, counts):
        yield word, sum(counts)


    def reducer_init(self):
        self.i = -1
        self.words = set()
        with open(self.options.database) as f:
            for line in f:
                word = line.split()[1]
                word = word.replace('"', '')
                self.words = self.words | {word}


    def reducer(self, word, counts):
        if word not in self.words and sum(counts) > 40:
            self.i += 1
            yield word, self.i


if __name__ == '__main__':
    FindAllButTop40.run()