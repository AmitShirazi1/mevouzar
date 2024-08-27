
from mrjob.job import MRJob
from mrjob.step import MRStep

class hw1(MRJob):
  # Condition 1:
  def filter_airtime(self, time):
    return int(time) >= 70000 and int(time) < 90000

  # Condition 2:
  def filter_genre(self, genre):
    flag = False
    for g in ['Talk', 'Politics', 'Spanish', 'Community', 'Martial arts']:
      if g in genre:
          flag = True
    return flag
  
  # Condition 3:
  def filter_title(self, title):
    flag = False
    for let in ['j', 'q', 'z']:
      if let in title:
        flag = True
    return flag

  def mapper(self, _, txt_row):
    row = txt_row.split("||")
    if row[0] != 'prog_code':
      if self.filter_airtime(row[4]) and self.filter_genre(row[2]) and self.filter_title(row[1].lower()):
        yield (row[1], row[2]), row[3]

  def reducer(self, title_genre, dates):
    yield title_genre, len(set(dates))
  
if __name__ == '__main__':
  hw1.run()
