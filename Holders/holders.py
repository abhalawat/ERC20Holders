import asyncio
import json
import eth_abi
import ray
from web3 import Web3
from web3.middleware import geth_poa_middleware
from ray.util import inspect_serializability
import pymongo

infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

connection_url = 'mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
Database = client.get_database('Holders')

def mongo(addressTo):
    connection_url = 'mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connection_url)
    Database = client.get_database('Holders')
    holders = Database.holders
    queryObject = {
                'addressTo':addressTo,
                }
    if holders.find_one({'addressTo': addressTo}) == None:
        holders.insert_one(queryObject)
    print(addressTo)
    

@ray.remote
def holdersEvent(_fromBlock, _toBlock,address):
    infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    abi = json.load(open('erc20abi.json','r'))
    contract = web3.eth.contract(address=address,abi=abi)
    try:
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        try:  
            for i in range(len(transferEvents.get_all_entries())):
                print(transferEvents.get_all_entries()[i].args.value)
                addressTo = transferEvents.get_all_entries()[i].args.to
                #print(addressTo)
                mongo(addressTo)
        except eth_abi.exceptions.InsufficientDataBytes:
            pass           
    except asyncio.TimeoutError: 
        pass
    
if __name__=="__main__":
    ray.init(address='auto')
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    holdersProcess = []
    Database = client.get_database('Holders')
    fromToDetails = Database.fromToDetails
    query = fromToDetails.find()
    output={}
    i=0
    details =[]
    for x in query:
        details.append(x)
    #inspect_serializability(holdersEvent, name="holdersEvent")
    for l in details:
        #_fromBlock, _toBlock,address
        holdersProcess.append(holdersEvent.remote(l['from'], l['to'],l['contractAddress']))
    
    #erc20Process = ray.put(erc20Process)
    print(ray.get(holdersProcess))
    