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


class Restaurant:
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
    def toTupleList((businessid, attributes), listOfAttributes):
        lst = []
        for item in listOfAttributes.value:
            if item in attributes:
                if attributes[item] is True:
                    lst.append((businessid, item, 1))
                elif attributes[item] is False:
                    lst.append((businessid, item, 0))
                else:
                    lst.append((businessid, item, attributes[item]))
            else:
                lst.append((businessid, item, 'NA'))
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
