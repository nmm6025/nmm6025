from mrjob.job import MRJob

class Q1WordCount(MRJob):

    def mapper(self, _, line):
        for word in line.split():
            # Only emit the word if it contains 'f' or 't'
            if 'f' in word or 't' in word:
                yield (word, 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    Q1WordCount.run()


