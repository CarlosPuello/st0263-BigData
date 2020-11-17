from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
       for w in line.decode('utf-8', 'ignore').split():
        vec=  line.split(',')
        se= vec[1]
        sa= int(vec[2])
        yield se,sa

    def reducer(self, key, values):
        lista=list(values)   # generador..estructura q se usa una sola vez
        prom=sum(lista)/len(lista)
        yield key, prom

if __name__ == '__main__':
    MRWordFrequencyCount.run()
