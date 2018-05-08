from mrjob.job import MRJob 
import json
import re

word_dict = {}
with open("output.txt") as f:
    for line in f:
    	word = line.split()[1]
    	word = word.replace('"', '')
    	word_dict[word] = None


class FindAllButTop40(MRJob):

    def mapper(self, _, line):
        review = json.loads(line)
        words = re.findall(r'[a-zA-Z]+', review['reviewText'])
        for word in words:
            yield word.lower(), 1


    def combiner(self, word, counts):
        yield word, sum(counts)


    def reducer(self, word, counts):
        summ = sum(counts)
        if word not in word_dict and summ > 40:
        	yield word, summ

if __name__ == '__main__':
    FindAllButTop40.run()