import json
"""
This program adds a cluster id

"""
class AddClusterId:

    def __init__(self):
        pass

    def f(self,x,y):
        if isinstance(y,dict):
            jsonObj = y
        else:
            jsonObj = json.loads(y)
        print jsonObj

    def perform(self,rdd):
        rdd = rdd.flatMapValues(lambda x : self.addClusterId(x))
        #rdd = rdd.foreach(lambda (x,y) : self.f(x,y))
        return rdd

    def addClusterId(self,line):
        """
        :param line:
         reads all the ads in particular cluster, takes the hash of those uri's and appends that as cluster_id
        """
        if isinstance(line,dict):
            jsonObj = line
        else:
            jsonObj = json.loads(line)
        concat_str=''
        for item in jsonObj['cluster']:
            concat_str += item['uri']
        cluster_id = "http://dig.isi.edu/ht/data/" + str(hash(concat_str)% 982451653)
        for item in jsonObj['cluster']:
            item['cluster_id']=cluster_id
            yield item