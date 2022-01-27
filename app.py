import json
from web3 import Web3
from web3.middleware import geth_poa_middleware
infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
block = web3.eth.get_block(14068095)
transaction = block.transactions
# oneHash = transaction[1].hex()
# print(oneHash)
# oneHashDetails = web3.eth.getTransactionReceipt(oneHash)
# print(oneHashDetails.contractAddress)
for i in range(len(transaction)):
    tx_hash = transaction[i].hex()
    print(tx_hash," ",web3.eth.getTransactionReceipt(tx_hash).contractAddress)

# 0xf13a3c819434bf3d9363ecfae3891c36c4cae4776a78af35e3fa71c59b6c2c8f
# 0xF31a4911cA351847b566E992179f7e8647b17FAf

# # #if loop to extracting contract address.
# abi = json.load(open('erc20abi.json','r'))

# #address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
# address = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
# contract = web3.eth.contract(address=address,abi = abi)
# # print(contract.events)
# # for event in contract.events:
# #     print(event)
# transferEvents = contract.events.Transfer.createFilter(fromBlock=14086188,toBlock=14086304)
# print(transferEvents.get_all_entries()[0].args.value)

# def erc20Details(adddress,_fromBlock,_toBlock):
#     abi = json.load(open('erc20abi.json','r'))
#     contract = web3.eth.contract(address=address,abi = abi)
#     transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock,toBlock=_toBlock)

