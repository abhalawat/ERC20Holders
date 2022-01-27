import json
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

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
            holdersEvent(fromBlock,toBlock,contract,address)
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            holdersEvent(fromBlock,toBlock,contract,address)
    else:
        holdersEvent(firstBlock,latest,contract,address)

def holdersEvent(_fromBlock, _toBlock,contract,address):
    try:
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        for i in range(len(transferEvents.get_all_entries())):
            try:
                transferEvents = contract.events.Transfer.createFilter(fromBlock=14086188,toBlock=14086304)
                print(transferEvents.get_all_entries()[0].args.value)
                addressTo = transferEvents.get_all_entries()[i].args.to
                print(addressTo)
                 #collection.insert_one({"Contractaddress":address,"holderAddress": addressTo})
            except eth_abi.exceptions.InsufficientDataBytes:
                pass           
    except asyncio.TimeoutError: 
        pass

def main():
    #itterating from block zero to latest
    #using trasaction of one block
    #from multiple trsaction of that block choose the trsaction where contract address is not null
    #use that contract address to check if it is erc20
    
    latestBlock = web3.eth.get_block('latest').number
    for block in range(latestBlock):
        blockDetails = web3.eth.get_block(block)
        transaction = blockDetails.transactions
        for i in range(len(transaction)):
            tx_hash = transaction[i].hex()
            address = web3.eth.getTransactionReceipt(tx_hash).contractAddress
            if address == "None":
                pass
            else:
                holdersContract(address,block)

     

if __name__=="__main__":
    main()