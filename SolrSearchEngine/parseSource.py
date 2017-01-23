import os
import re
ls = os.listdir(".")


def getTokens(line):
    newtokens = []

    tokens = line.strip().split(':', 1)

    for token in tokens:
        newtokens.append(re.sub('[\s+]', ' ', token.strip(' \t')))

    return newtokens


def process(f, jsonfilename):
    filecontents = f.readlines()
    state = "StartTitle"
    o = open(jsonfilename, 'w')
    o.write("{\n")

    for i in range(len(filecontents)):
        line = filecontents[i]

        tokens = getTokens(line)

        rtnline = ""
        if state == "StartTitle":
            rtnline += '"' + tokens[0] + '": "' + tokens[1]
            state = "GetTitle"
        elif state == "GetTitle":
            if len(tokens) == 1:
                rtnline += tokens[0]
            else:
                rtnline += '",\n'
                state = "Normal"
                rtnline += '"' + tokens[0] + '": "' + tokens[1] + '",\n'
        elif state == "Normal":
            if len(tokens) == 1:
                if tokens[0] == "":
                    continue
                elif tokens[0] in ["Latest", "Expected"]:
                    rtnline += '"' + tokens[0] + " "
                    state = "ContinueKey"
            else:
                if tokens[0] in ["Fld Applictn", "Prgm Manager", "Sponsor"]:
                    rtnline += '"' + tokens[0] + '": "' + tokens[1] + '\\n'
                    state = "ContinueValue"
                elif tokens[0] == "Abstract":
                    rtnline += '"' + tokens[0] + '": "'
                    state = "GetAbstract"
                else:
                    rtnline += '"' + tokens[0] + '": "' + tokens[1] + '",\n'

        elif state == "ContinueKey":
            if len(tokens) == 1:
                rtnline += tokens[0] + " "
            else:
                rtnline += tokens[0] + '": "' + tokens[1] + '",\n'
                state = "Normal"

        elif state == "ContinueValue":
            if len(tokens) == 1:
                rtnline += tokens[0] + "\\n"
            else:
                rtnline += '",\n'
                state = "Normal"
                rtnline += '"' + tokens[0] + '": "' + tokens[1] + '",\n'
        elif state == "GetAbstract":
            if i == len(filecontents) - 1:
                rtnline += tokens[0] + '"\n}'
            else:
                rtnline += tokens[0] + '\\n'
        o.write(rtnline)

for filen in ls:
    if filen == "parseSource.py" or filen == ".idea":
        continue
    jsonfilename = filen.replace(".txt",".json")
    f = open(filen, 'r')
    process(f, jsonfilename)
    f.close()
