import json


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
        return str(parsed_json['business_id']), parsed_json['attributes']

    @staticmethod
    def get_attribute_list((businessid, attributes)):
        return attributes.keys()
