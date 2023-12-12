from mrjob.job import MRJob
from mrjob.step import MRStep

class FlightAnalysis(MRJob):

    def mapper(self, _, line):
        parts = line.strip().split(',')
        if parts[0] != 'itinerary_id':
            itinerary_id = int(parts[0])
            passengers = int(parts[1])
            yield itinerary_id, (1, passengers)

    def reducer(self, key, values):
        total_lines = 0
        max_passengers = 0

        for count, passengers in values:
            total_lines += count
            max_passengers = max(max_passengers, passengers)
        if total_lines >= 2:
            yield key, (total_lines, max_passengers)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    FlightAnalysis.run()
