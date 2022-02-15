from web3 import Web3
import pymongo
import ray
from ray.util import inspect_serializability
import gc


@ray.remote
class DataBase(object):
  def __init__(self):
    self.Block_Addres = pymongo.MongoClient('mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority').get_database('Holders').Block_Address

  def mongo(self,contractAddress, block): 
      if self.Block_Addres.find_one({'contractAddress': contractAddress}) == None:
          self.Block_Addres.insert_one({
              'contractAddress': contractAddress,
              'Block': block
                  })
      gc.collect()



@ray.remote
def contract(_block):
    #infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
    for i in range(len(web3.eth.get_block(_block).transactions)):
        #tx_hash = web3.eth.get_block(_block).transactions[i].hex()
        address = web3.eth.getTransactionReceipt(web3.eth.get_block(_block).transactions[i].hex()).contractAddress
        #print("address ",address," txHash ",tx_hash)
        if address != None:
            database.mongo.remote(address, _block)
    gc.collect()

            

if __name__=="__main__":
    #inspect_serializability(contract, name="contract")
    ray.init(address='auto')
    #ray.init()
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    latestBlock = web3.eth.get_block('latest').number
    del web3
    del infura_url
    database = DataBase.remote()
    ray.get([contract.remote(block) for block in reversed(range(latestBlock))])
    

