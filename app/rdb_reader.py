import codecs
import os


class RDB_PARSER:
    def __init__(self, directory, filename):
        self.directory = directory
        self.filename = filename
        rdbfile = f"{self.directory}/{self.filename}"
        print(f'rdbfile: {rdbfile}')
        print(f'rdbfile: {rdbfile}')

    def readDB(self):
        if os.path.exists(f"{self.directory}/{self.filename}"):
            with open(f"{self.directory}/{self.filename}", "rb") as f:
                data = f.read()
                print(f'data:{data}')
            return data
        else:
            print(f'data is not exists')
            return b""

    def trimTheContent(self, data):
        if data == b"":
            return None
        res = data.hex(" ")
        idxofb = res.index("fb")
        idxoff = res.index("ff")
        lst = res[idxofb:idxoff].split("00")
        reslst = []
        for x in lst:
            reslst.append(str(x).strip().split(" "))
        reslst.pop(1)
        return reslst

    def getTheHashSize(self, data):
        return data[0][1]

    def extractTheKeyValuePairs(self, data):
        if data is None:
            return None
        data.pop(0)
        result = {}
        for x in data:
            lengthKey = int(x[0])
            l1 = x[1 : lengthKey + 1]
            l2 = x[lengthKey:]
            key = ""
            value = ""
            value_integer = False
            for byt in l1:
                key += byt
            if "c0" in l2[0]:
                value_integer = True
                value = int(l2[1], 16)
                break
            else:
                l2.pop(0)
                for byt in l2:
                    value += byt
            key = codecs.decode(key, "hex").decode("utf-8")
            if not value_integer:
                try:
                    value = codecs.decode(value, "hex").decode("utf-8")
                except UnicodeDecodeError:
                    value = "Invalid UTF-8"
            result[f"{key}"] = f"{value}"
        return result

    def getKeys(self):
        print("this is reaching getKeysFunction in RDBPARSER")
        data = self.readDB()
        if data == b"":
            return None
        trimmedData = self.trimTheContent(data)
        key_dict = self.extractTheKeyValuePairs(trimmedData)
        print("key received", key_dict)
        result = []
        for k in key_dict:
            result.append(k)
        return result

