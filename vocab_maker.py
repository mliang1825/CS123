from mrjob.job import MRJob 
import json
import re
import heapq

class FindTop40(MRJob):

    def mapper(self, _, line):
        review = json.loads(line)
        words = re.findall(r'[a-zA-Z]+', review['reviewText'])
        for word in words:
            yield word.lower(), 1


    def combiner(self, word, counts):
        yield word, sum(counts)


    def reducer_init(self):
        self.heap = [(0, [])] * 40
        self.word_dict = {}
        heapq.heapify(self.heap)

    def reducer(self, word, counts):
        summ = sum(counts)
        if summ > self.heap[0][0]:
            heapq.heapreplace(self.heap, (summ, word))

    def reducer_final(self):
        for entry in self.heap:
            yield entry


if __name__ == '__main__':
    FindTop40.run()