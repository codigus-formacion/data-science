class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.results = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.results.append(value) 

    def execute(self, data, mapper, reducer):
        for line in data.readlines():
            mapper(line)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        for result in self.results:
            print(result)