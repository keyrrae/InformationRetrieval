from __future__ import print_function
import json
from pyspark import AccumulatorParam


class Business:

    def __init__(self):
        self.id = ""

    @staticmethod
    def to_string(line):
        return str(line)

    @staticmethod
    def is_res(line):
        parsed_json = json.loads(line)

        return u'Restaurants' in parsed_json['categories']

    @staticmethod
    def get_id(line):
        parsed_json = json.loads(line)

        return str(parsed_json['business_id'])

    @staticmethod
    def get_id_attributes(line):
        parsed_json = json.loads(line)
        attribDict = {}
        for key, value in parsed_json['attributes'].iteritems():
            if type(value) == dict:
                for subkey, subvalue in value.iteritems():
                    attribDict[subkey] = subvalue
            else:
                attribDict[key] = value
        return str(parsed_json['business_id']), attribDict

    @staticmethod
    def get_attribute_list((businessid, attributes)):
        return attributes.keys()


class Restaurant(Business):
    def __init__(self):
        self.id = ""

    @staticmethod
    def toRowWithNA((businessid, attributes), listOfAttributes):
        lst = []
        for item in listOfAttributes.value:
            if item in attributes:
                if attributes[item] is True:
                    lst.append(1)
                elif attributes[item] is False:
                    lst.append(0)
                else:
                    lst.append(attributes[item])
            else:
                lst.append('NA')
        return businessid, lst  # returns a tuple of businessid and its attributes row vector

    @staticmethod
    def toTupleWithNA((businessid, attributes), listOfAttributes):
        lst = []
        for item in listOfAttributes.value:
            if item in attributes:
                if attributes[item] is True:
                    lst.append((item, 1))
                elif attributes[item] is False:
                    lst.append((item, 0))
                else:
                    lst.append((item, attributes[item]))
            else:
                lst.append((item, 'NA'))
        return businessid, lst  # returns a tuple of businessid and its attributes row vector

    @staticmethod
    def toTupleList((businessid, attributes)):
        lst = []
        for i in xrange(len(attributes)):
            lst.append((businessid, i, attributes[i]))
        return lst  # returns a tuple of businessid and its attributes row vector

    @staticmethod
    def collectAttributeValues(AttribSet,AttribValMatrix):
        retCol = [[] for i in xrange(len(AttribValMatrix[0]))]
        for row in AttribValMatrix:
            for i in xrange(len(row)):
                if row[i] not in retCol[i]:
                    retCol[i].append(row[i])
        for i in xrange(len(retCol)):
            retCol[i] = (AttribSet[i], retCol[i])
        return retCol

    @staticmethod
    def printNonBinaryAttrValPairs(AttribValPair):
        for Attr, Val in AttribValPair:
            if set(Val) != {0, 1 ,'NA'}:
                print((Attr, Val))

    @staticmethod
    def buildID_NumReferences(inputList):
        getNum = {}
        getID = {}
        for i in xrange(len(inputList)):
            getID[i] = inputList[i]
            getNum[inputList[i]] = i

        return getNum, getID

    @staticmethod
    def reconstructVector((id, attribList)):
        resList = [-1.0 for i in xrange(62)]
        for i in xrange(0, len(attribList), 2):
            resList[attribList[i]] = attribList[i + 1]
        return id, resList

    @staticmethod
    def removeNegValues((id, attribList)):
        resList = []
        for item in attribList:
            if item <= 0:
                resList.append(0)
            else:
                resList.append(item)
        return id, resList

    @staticmethod
    def toNumeric((businessID, attributeVal), restGetNumBC, attribSetBC):

        retAttribVal = attributeVal[:]
        attribList = attribSetBC.value
        for i in xrange(len(attribList)):
            if i == 1:
                if attributeVal[i] == u'allages':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'18plus':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'19plus':
                    retAttribVal[i] = 2.0
                elif attributeVal[i] == u'21plus':
                    retAttribVal[i] = 3.0
                else:
                    retAttribVal[i] = -1.0

            elif i == 2:
                if attributeVal[i] == u'none':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'beer_and_wine':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'full_bar':
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0

            elif i == 3:
                if attributeVal[i] == u'casual':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'dressy':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'formal':
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0

            elif i == 5:
                if attributeVal[i] == u'no':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'yes_free':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'yes_corkage':
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0

            elif i == 18:
                if attributeVal[i] == u'quiet':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'average':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'loud':
                    retAttribVal[i] = 2.0
                elif attributeVal[i] == u'very_loud':
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0
            elif i == 22:
                if attributeVal[i] == 1:
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == 2:
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == 3:
                    retAttribVal[i] = 2.0
                elif attributeVal[i] == 4:
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0
            elif i == 23:
                if attributeVal[i] == u'no':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'outdoor':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'yes':
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0
            elif i == 28:
                if attributeVal[i] == u'no':
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == u'free':
                    retAttribVal[i] = 1.0
                elif attributeVal[i] == u'paid':
                    retAttribVal[i] = 2.0
                else:
                    retAttribVal[i] = -1.0
            else:
                if attributeVal[i] == 0:
                    retAttribVal[i] = 0.0
                elif attributeVal[i] == 1:
                    retAttribVal[i] = 1.0
                else:
                    retAttribVal[i] = -1.0

        return restGetNumBC.value[businessID], retAttribVal

'''
(u'Ages Allowed', ['NA', u'21plus', u'allages', u'18plus', u'19plus'])
(u'Alcohol', [u'none', u'full_bar', 'NA', u'beer_and_wine'])
(u'Attire', [u'casual', 'NA', u'dressy', u'formal'])
(u'BYOB/Corkage', ['NA', u'yes_free', u'no', u'yes_corkage'])
(u'Noise Level', [u'average', u'loud', 'NA', u'quiet', u'very_loud'])
(u'Price Range', [1, 2, 'NA', 3, 4])
(u'Smoking', ['NA', u'no', u'outdoor', u'yes'])
(u'Wi-Fi', ['NA', u'no', u'free', u'paid'])
'''



class attributeAccumulatorParam(AccumulatorParam):
    def zero(self, listOfAttributes):
        return str, [[] for i in xrange(len(listOfAttributes.value))], listOfAttributes

    def addInPlace(self, v1, v2):
        # print type(v1), type(v2)
        try:
            for i in xrange(len(v2[1].value)):
                if v2[1].value[i] in v2[0][1]:
                    if v2[0][1][v2[1].value[i]] not in v1[i]:
                        v1[i].append(v2[0][1][v2[1].value[i]])
        except:
            print(v1)
            print(v2)
        return v1
