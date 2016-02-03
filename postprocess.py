
inputf = open("output", 'r')
outputf = open("results", 'w')

lines = inputf.readlines()
keyValuePairs = []

for line in lines:
    temp = line.strip().split()
    if len(temp) != 2:
        continue

    keyValuePairs.append((temp[0], int(temp[1])))

keyValuePairs.sort(key=lambda tup:tup[1], reverse=True)


length = 5 if len(keyValuePairs) >= 5 else len(keyValuePairs)

for i in xrange(length):
    print >>outputf, "{0}\t{1}".format(keyValuePairs[i][0], keyValuePairs[i][1])

inputf.close()
outputf.close()
