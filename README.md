To Create 3 local cluster

Open 3 terminals

On 1st Terminal:
RUN: ray start --head --node-ip-address=192.168.1.220
This will create head node

On 2nd Terminal:
RUN: ray start --address='192.168.1.220:6379' --redis-password='5241590000000000'
This will create non-head node

On 3rd Terminal:
RUN: ray start --address='192.168.1.220:6379' --redis-password='5241590000000000'
This will create non-head node

Then run the python files in order:-
ContractAddress/ python3 address.py
ERC20Address/ python3 erc20Address.py
Holders/ holders.py