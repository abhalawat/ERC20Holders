from web3 import Web3
import pymongo
import ray
from ray.util import inspect_serializability
from web3.middleware import geth_poa_middleware
import gc
import asyncio



def mongo(contractAddress, block):  
    Block_Address = pymongo.MongoClient('mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority').get_database('Holders').Block_Address
    if Block_Address.find_one({'contractAddress': contractAddress}) == None:
        print("inserted")
        Block_Address.insert_one({
        'contractAddress': contractAddress,
        'block': block
    })
    del Block_Address
    gc.collect()



@ray.remote
def contract(block):
    #infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider("wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    try:
        for tx_hash in web3.eth.get_block(block).transactions:
            #tx_hash = web3.eth.get_block(_block).transactions[i].hex()
            address = web3.eth.getTransactionReceipt(tx_hash).contractAddress
            #print("address ",address," txHash ",tx_hash)
            if address != None:
                mongo(address, block)
    except asyncio.TimeoutError: 
        pass
        
    del web3
    del block
    gc.collect()
   

            

if __name__=="__main__":
    #inspect_serializability(contract, name="contract")
    #ray.init()
    
    infura_url= "wss://eth-mainnet.alchemyapi.io/v2/NMMzTK9vae0CA0DrxtR_TiqCHkf3qkqD"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    ray.init(address='auto', _redis_password='5241590000000000')
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    block= web3.eth.get_block('latest').number + 1
    del web3
    del infura_url
    while block>=0:
        block-=1
        contract.remote(block)
    #ray.get([contract.remote(block) for block in reversed(range(latestBlock))])


