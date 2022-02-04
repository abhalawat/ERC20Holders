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
        except eth_abi.exceptions.InsufficientDataBytes:
            pass           
    except asyncio.TimeoutError: 
        pass
    
if __name__=="__main__":
    with open('blocks_address.json') as f:
        blocks_address = json.load(f)

    
    fromBlock = 0
    toBlock = 1
    address = 2
    
    holdersProcess = []
    #inspect_serializability(holdersEvent, name="holdersEvent")
    for i in range(len(blocks_address)):
        fblock = int(blocks_address[i][fromBlock])
        toblock = blocks_address[i][toBlock]
        address = blocks_address[i][address]
        holdersProcess.append(holdersEvent.remote(fblock,toblock,address))
    
    #erc20Process = ray.put(erc20Process)
    print(ray.get(holdersProcess))
    