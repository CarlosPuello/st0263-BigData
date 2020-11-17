from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
       for w in line.decode('utf-8', 'ignore').split():
        vector = line.split(',')
	empleado=vector[0]
        sector=vector[1]
        yield empleado,sector

    def reducer(self, key, values):
        lista=list(values)
        suma=len(lista)
        yield key, suma

if __name__ == '__main__':
    MRWordFrequencyCount.run()
