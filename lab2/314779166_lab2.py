
from mrjob.job import MRJob
from mrjob.step import MRStep

class lab_02(MRJob):

  def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_max_program)
        ]
  
  def mapper(self, key, row):
    yield (row.split(","))[5], 1

  def reducer(self, program, counts):
    yield None, (sum(counts), program)

  def reducer_find_max_program(self, _, count_program_pairs):
    #count_program_pairs is a list of tuples
    yield max(count_program_pairs)
  
if __name__ == '__main__':
  lab_02.run()
