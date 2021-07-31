from module.reshard.common.measure import convert


class Cluster:
    def __init__(self, name):
        self.clustername = name
        self.count = 0
        self.network_transmission = "0kb"

    def move(self, storeValue):
        self.count = self.count + 1
        self.network_transmission = str(convert(self.network_transmission) + convert(storeValue)) + "kb"
