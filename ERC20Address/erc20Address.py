import asyncio
import json
import eth_abi
import ray
from web3 import Web3
from web3.middleware import geth_poa_middleware
from ray.util import inspect_serializability
import pymongo


infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

connection_url = 'mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
Database = client.get_database('Holders')

def mongo(address, fromBlock, toBlock):
    connection_url = 'mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connection_url)
    Database = client.get_database('Holders')
    fromToDetails = Database.fromToDetails
    queryObject = {
                'from':fromBlock,
                'to': toBlock,
                'contractAddress': address
            }
    print(address, fromBlock,toBlock)
    fromToDetails.insert_one(queryObject)

@ray.remote
def erc20(block,address):
    infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    latest = web3.eth.blockNumber
    firstBlock = block
    totalResult = latest - firstBlock
    initial = firstBlock
    if totalResult >2000:
        while totalResult>=2000:
            fromBlock = initial
            toBlock = initial +2000
            mongo(address, fromBlock, toBlock)
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            mongo(address, fromBlock, toBlock)
    else:
        mongo(address, firstBlock, latest)
        


    
if __name__=="__main__":
    erc20Process = []
    Database = client.get_database('Holders')
    Block_Address = Database.Block_Address
    query = Block_Address.find()
    output={}
    i=0
    details =[]
    for x in query:
        details.append(x)
    #print(type(details[1]['Block']))
    for l in details:
        erc20Process.append(erc20.remote(l['Block'], l['contractAddress']))
    ray.get(erc20Process)
    #inspect_serializability(holdersEvent, name="holdersEvent")

    

