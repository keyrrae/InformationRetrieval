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
    def getuserid(line):
        parsed_json = json.loads(line)
        return str(parsed_json['user_id'])

    @staticmethod
    def getbusiid(line):
        parsed_json = json.loads(line)
        return str(parsed_json['business_id'])

    @staticmethod
    def mapper(line):
        parsed_json = json.loads(line)
        key = str(parsed_json['user_id'])
        value = (str(parsed_json['business_id']), int(parsed_json['stars']))

        return key, value

    @staticmethod
    def parseRatings(line):
        parsed_json = json.loads(line)
        key = int(str(parsed_json['date'])[-1])
        value = (str(parsed_json['user_id']), str(parsed_json['business_id']), int(parsed_json['stars']))
        return key, value

    @staticmethod
    def getUsrResStar(line):
        parsed_json = json.loads(line)
        user = str(parsed_json['user_id'])
        busi = str(parsed_json['business_id'])
        star = int(parsed_json['stars'])
        return user, busi, star

    @staticmethod
    def normalizeStar(userAvgDict,(user, busi, star)):
        return user, busi, star - userAvgDict[user]

    @staticmethod
    def replaceIDwithNum((user, busi, star), idToNumDictBC, restGetIDBC):
        newBusi = 0
        newUser = 0
        if user in idToNumDictBC.value:
            newUser = idToNumDictBC.value[user]
        if busi in restGetIDBC.value:
            newBusi = restGetIDBC.value[busi]
        return newUser, newBusi, star

    @staticmethod
    def subtractAvg((user, busi, star), usrRatingAvgBC):
        newStar = 0.0
        if user in usrRatingAvgBC.value:
            newStar = star - usrRatingAvgBC.value[user]
        return user, busi, newStar

    @staticmethod
    def normalize((user, busiStars)):
        sum = 0
        for item in busiStars:
            busi, star = item
            sum += star

        avg = sum / float(len(busiStars))
        res = []
        for item in busiStars:
            busi, star = item
            res.append((busi, star - avg))
        return user, avg, res

    @staticmethod
    def flatten((user, avg, busiStars)):
        retstr = ""
        for i, (busi, star) in enumerate(busiStars):
            if i < len(busiStars) - 1:
                retstr += '(' + user + ',' + str(busi) + ',' + str(star) + "),"
            else:
                retstr += '(' + user + ',' + str(busi) + ',' + str(star) + ')'
        return retstr

    @staticmethod
    def vectorize(line):
        parsed_line = line.strip().strip("()").split(',')
        print len(parsed_line)
        return parsed_line[0], parsed_line[1], float(parsed_line[2])

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
        if type(accum) != list:
            temp = [accum]
            return temp
        else:
            accum.append(value)
            return accum

    @staticmethod
    def reshape((user, busiStars)):
        if type(busiStars) != list:
            res = [busiStars]
        else:
            res = busiStars
        return user, res


