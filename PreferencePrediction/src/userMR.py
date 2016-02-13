import json


class User:

    def __init__(self):
        self.id = ""

    @staticmethod
    def toString(line):
        return str(line)

    @staticmethod
    def getFriends(line):
        parsed_json = json.loads(line)
        return parsed_json['user_id'], parsed_json['friends']
