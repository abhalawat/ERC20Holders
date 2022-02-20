from web3 import Web3
from web3.middleware import geth_poa_middleware
import ray
from ray.util import inspect_serializability
import pymongo
import gc


def mongo(contractAddress, block):  
    client = pymongo.MongoClient('mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority')
    Database = client.get_database('Holders')
    Block_Address = Database.Block_Address
    if Block_Address.find_one({'contractAddress': contractAddress}) == None:
        Block_Address.insert_one({
        'contractAddress': contractAddress,
        'block': block
    })
    
    gc.collect()


@ray.remote
def contract(block):

        web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        for tx_hash in web3.eth.get_block(block).transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).contractAddress
            #print(contractAddress)
            if contractAddress != None:
                mongo(contractAddress, block)
                print(contractAddress, block)
        del web3
        del block
        gc.collect()


def start():   
    infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

    #ray.init(address='auto', _redis_password='5241590000000000')
    ray.init(address='auto')

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
          
start()