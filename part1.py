from mrjob.job import MRJob 
from mrjob.step import MRStep
import json
import re
import heapq

class FindAllButTop50(MRJob):

	def mapper_make_heap(self, _, line):
		review = json.loads(line)
		words = re.findall(r'[a-zA-Z]+', review['reviewText'])
		for word in words:
			yield word.lower(), 1


	def combiner_make_heap(self, word, counts):
		yield word, sum(counts)


	def reducer_make_heap_init(self):
		self.heap = [(0, [])] * 40
		heapq.heapify(self.heap)
		print("made heap")

	def reducer_make_heap(self, word, counts):
		summ = sum(counts)
		if summ > self.heap[0][0]:
			heapq.heapreplace(self.heap, (summ, word))
		yield word, summ

	def reducer_make_heap_final(self):
		self.word_dict = {}
		for entry in self.heap:
			self.word_dict[entry[1]] = None


	def reducer_filter_words(self, word, counts):
		if word not in self.word_dict:
			yield word


	def steps(self):
		return [
			MRStep(mapper=self.mapper_make_heap,
					combiner=self.combiner_make_heap, 
					reducer=self.reducer_make_heap),
			MRStep(reducer=self.reducer_filter_words)
			]

if __name__ == '__main__':
	FindAllButTop50.run()