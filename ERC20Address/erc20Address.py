import asyncio
import json
import eth_abi
import ray
from web3 import Web3
from web3.middleware import geth_poa_middleware
from ray.util import inspect_serializability

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

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
            #holdersProcess.append(holdersEvent.remote(fromBlock,toBlock,address))
            print(fromBlock,toBlock,address)
            details.append([fromBlock,toBlock,address])
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            details.append([fromBlock,toBlock,address])
            print(fromBlock,toBlock,address)
            # holdersProcess.append(holdersEvent.remote(fromBlock,toBlock,address))
    else:
        details.append([firstBlock,latest,address])
        print(firstBlock,latest,address)
        #holdersProcess.append(holdersEvent.remote(firstBlock,latest,address))

@ray.remote
def holdersEvent(_fromBlock, _toBlock,address):
    infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
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
                print(addressTo)
                #collection.insert_one({"Contractaddress":address,"holderAddress": addressTo})
        except eth_abi.exceptions.InsufficientDataBytes:
            pass           
    except asyncio.TimeoutError: 
        pass
    
if __name__=="__main__":
    with open('data.json') as f:
        block_addres = json.load(f)

    block_addreslist = list(block_addres.items())
    blockIndex = 0
    addressIndex = 1
    print(len(block_addreslist))
    erc20Process = []
    holdersProcess = []
    details = []
    #inspect_serializability(holdersEvent, name="holdersEvent")
    for i in range(len(block_addreslist)):
        block = int(block_addreslist[i][blockIndex])
        address = block_addreslist[i][addressIndex]
        erc20Process.append(erc20.remote(block,address))
    
    #erc20Process = ray.put(erc20Process)
    print(ray.get(erc20Process))
    # holdersProcess = ray.put(holdersProcess)
    # print(ray.get(holdersProcess))
    print(details)

