import json


class Review:

    def __init__(self):
        self.id = ""

    @staticmethod
    def toString(line):
        return str(line)

    @staticmethod
    def is_res(line, reslist):
        parsed_json = json.loads(line)

        return parsed_json['business_id'] in reslist.value

    @staticmethod
    def mapper(line):
        parsed_json = json.loads((line))
        key = str(parsed_json['user_id'])
        value = (str(parsed_json['business_id']), int(parsed_json['stars']))

        return key, value

    @staticmethod
    def combiner(a):    # Turns value a (a tuple) into a list of a single tuple.
        return [a]

    @staticmethod
    def merge_value(a, b):  # a is the new type [(,), (,), ..., (,)] and b is the old type (,)
        a.extend([b])
        return a

    @staticmethod
    def merge_combiners(a, b):  # a is the new type [(,),...,(,)] and so is b, combine them
        a.extend(b)
        return a

    @staticmethod
    def reducer(accum, value):
        if type(accum) == tuple:
            temp = [accum]
            return temp
        else:
            accum.append(value)
            return accum
