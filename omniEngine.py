#from rpcclient import *
from sql import *

#host=RPCHost()
dbc=sql_connect()

#addr="1Po1oWkD2LmodfkBYiAktwh76vkF93LKnh"
#hash="00000000000000001ca7d646d1fbc237c94ecc9572ca21207d1140139b9e380c"
#tx="184188c2a742daec94763927c91b16f347510bc32e3525b6ff2ba431ebcb2252"

#select(dbc)
#exit(1)

#csv output file info
fieldnames = ['TxHash', 'protocol', 'TxType', 'TxVersion', 'Ecosystem', 'TxSubmitTime', 'TxState', 'TxErrorCode', 'TxBlockNumber', 'TxSeqInBlock', 'TxBlockTime', 'TxMsg']
out_file = open('data/316639.current.csv', "wb") 
writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
writer.writerow(dict((fn,fn) for fn in fieldnames))

currentBlock=316639
#endBlock=316593

#get last known block from the RPC client
endBlock=getinfo()['result']['blocks']


while currentBlock <= endBlock:
 try:
  #address_data=host.call("getallbalancesforaddress_MP", addr)
  hash = getblockhash(currentBlock)['result']
  block_data = getblock(hash)
  height = block_data['result']['height']
  block_data_MP = listblocktransactions_MP(height)
  print "Processing Block Height", height, "of", endBlock

  #prime tx sequence number based on number of tx
  x=len(block_data['result']['tx'])
  print "Found ", x, "Bitcoin transactions"
  for tx in block_data['result']['tx']:
    rawtx=getrawtransaction(tx)
    #insert_transacation(dbc, rawtx, "Bitcoin", height)
    dump_csv(writer, rawtx, "Bitcoin", height, x)
    #decrement tx sequence number in block
    x-=1

  #prime tx sequence number based on number of tx
  x=len(block_data_MP['result'])
  print "Found ", x, "Mastercoin transactions"
  for tx in block_data_MP['result']:
    rawtx=gettransaction_MP(tx)
    #insert_transacation(dbc, rawtx, "Mastercoin", height)
    dump_csv(writer, rawtx, "Mastercoin", height, x)
    #decrement tx sequence number in block
    x-=1

  #rawtx=host.call("getrawtransaction", tx, 1)
  #print json.dumps(block_data, indent=2, sort_keys=True)
  #print json.dumps(rawtx, indent=2, sort_keys=True)
  #dump_csv(writer, rawtx, "Bitcoin", height)


 except Exception,e:
  print "Problem with ", e

 #increment to next block
 currentBlock += 1


#print json.dumps(block_data,indent=2)
#print address_data

