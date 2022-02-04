import json
from web3 import Web3
from web3.middleware import geth_poa_middleware
import pymongo
from pymongo import MongoClient
import ray
from ray.util import inspect_serializability

ray.init()

cluster = MongoClient("mongodb://127.0.0.1:27017")

db = cluster['ERC20']
collection = db['Block&Address']
infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

Block_Address = {}

@ray.remote
def contract(_block):
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    blockDetails = web3.eth.get_block(_block)
    transaction = blockDetails.transactions
    for i in range(len(transaction)):
        tx_hash = transaction[i].hex()
        address = web3.eth.getTransactionReceipt(tx_hash).contractAddress
        print("address ",address," txHash ",tx_hash)
        if address != None:
            #collection.insert_one({"Contractaddress":address,"Block": _block})
            Block_Address[_block] = address
            return address

#inspect_serializability(contract, name="contract")

latestBlock = web3.eth.get_block('latest').number
futures = [contract.remote(block) for block in range(latestBlock)]
futures_id = ray.put(futures)
print(ray.get(futures))


a_file = open("data.json", "w")

json.dump(Block_Address, a_file)

a_file.close()

