class a(object):
    def __init__(self):
        self.a = 'yyyy'

    def preprocess(self):
        self.process()

    def process(self):
        raise NotImplementedError('Provide a way to get the config!')


class b(a):
    def process(self):
        print('ss')

class c(b):
    def aaa(self):
        print('ggggg')

c().preprocess()
