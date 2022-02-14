import json
from web3 import Web3
from web3.middleware import geth_poa_middleware
import pymongo
import ray
from ray.util import inspect_serializability



def mongo(contractAddress, block):
    # connection_url = 'mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority'
    # client = pymongo.MongoClient(connection_url)
    # Database = client.get_database('Holders')
    Block_Address = pymongo.MongoClient('mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority').get_database('Holders').Block_Address
    queryObject = {
    'contractAddress': contractAddress,
    'Block': block
    }
    if Block_Address.find_one({'contractAddress': contractAddress}) == None:
        Block_Address.insert_one(queryObject)



@ray.remote
def contract(_block):
    infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    for i in range(len(web3.eth.get_block(_block).transactions)):
        tx_hash = web3.eth.get_block(_block).transactions[i].hex()
        address = web3.eth.getTransactionReceipt(tx_hash).contractAddress
        #print("address ",address," txHash ",tx_hash)
        if address != None:
            mongo(address, _block)
            return address

if __name__=="__main__":
    #inspect_serializability(contract, name="contract")
    ray.init(address='auto')
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    latestBlock = web3.eth.get_block('latest').number
    del web3
    del infura_url
    # futures = [contract.remote(block) for block in reversed(range(latestBlock))]
    # print(ray.get(futures))
    ray.get([contract.remote(block) for block in reversed(range(latestBlock))])
    

