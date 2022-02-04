import json
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import pymongo
from pymongo import MongoClient
#from dask import delayed
import ray


cluster = MongoClient("mongodb://127.0.0.1:27017")

db = cluster['ERC20']
collection = db['erc20']
infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

future =[]

def holdersEvent(_fromBlock, _toBlock,contract,address):
    try:
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        for i in range(len(transferEvents.get_all_entries())):
            print(transferEvents.get_all_entries()[i].args.value)
            addressTo = transferEvents.get_all_entries()[i].args.to
            print(addressTo)
            collection.insert_one({"Contractaddress":address,"holderAddress": addressTo})
                 
    except asyncio.TimeoutError: 
        pass

def holdersContract(address,block):
    abi = json.load(open('erc20abi.json','r'))
    contract = web3.eth.contract(address=address,abi=abi)
    latest = web3.eth.blockNumber
    firstBlock = block
    totalResult = latest - firstBlock
    initial = firstBlock
    if totalResult >2000:
        while totalResult>=2000:
            fromBlock = initial
            toBlock = initial +2000    
            future.append(erc20.remote(fromBlock,toBlock,contract,address))
            #holdersEvent(fromBlock,toBlock,contract,address)
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            #holdersEvent(fromBlock,toBlock,contract,address)
            future.append(erc20.remote(fromBlock,toBlock,contract,address))
    else:
        #holdersEvent(firstBlock,latest,contract,address)
        future.append(erc20.remote(firstBlock,latest,contract,address))
#@delayed

@ray.remote
def erc20(_fromBlock, _toBlock,contract,address):
    try:
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        try:  
            print(transferEvents.get_all_entries()[1].args.value)
            #holdersEvent(_fromBlock,_toBlock,contract,address)
            return address
        except eth_abi.exceptions.InsufficientDataBytes:
            pass           
    except asyncio.TimeoutError: 
        pass


def blockDetail(block):
    blockDetails = web3.eth.get_block(block)
    transaction = blockDetails.transactions
    for i in range(len(transaction)):
        tx_hash = transaction[i].hex()
        address = web3.eth.getTransactionReceipt(tx_hash).contractAddress
        print("address ",address," txHash ",tx_hash)
        if address != None:
            #delayed(holdersContract)(address,block)
            holdersContract(address,block)

    

if __name__=="__main__":
    #itterating from block zero to latest
    #using transaction of one block
    #from multiple transaction of that block choose the transaction where contract address is not null
    #use that contract address to check if it is erc20
    ray.init()
    latestBlock = web3.eth.get_block('latest').number
    for block in reversed(range(latestBlock)):
        blockDetail(block)
    print(ray.get(future))