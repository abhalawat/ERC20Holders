from web3 import Web3
from web3.middleware import geth_poa_middleware
import pymongo
import ray
from ray.util import inspect_serializability



def mongo(contractAddress, block):
    connection_url = 'mongodb+srv://abhalawat:1234@cluster0.hnxq5.mongodb.net/Holders?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connection_url)
    Database = client.get_database('Holders')
    Block_Address = Database.Block_Address
    queryObject = {
    'contractAddress': contractAddress,
    'Block': block
    }
    if Block_Address.find_one({'contractAddress': contractAddress}) == None:
        Block_Address.insert_one(queryObject)
    print(Block_Address)



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
            mongo(address, _block)
            return address

if __name__=="__main__":
    #inspect_serializability(contract, name="contract")
    #ray.init()ray.init(log_to_driver=False)
    ray.init(address='auto', _redis_password='5241590000000000',log_to_driver=False)
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    latestBlock = web3.eth.get_block('latest').number
    futures = [contract.remote(block) for block in reversed(range(latestBlock))]
    futures_id = ray.put(futures)
    print(ray.get(futures))