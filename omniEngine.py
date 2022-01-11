from sql import *
import os
import sys
from datetime import datetime
from datetime import timedelta
from cacher import *
import config

USER=os.getenv("USER")

lockFile='/tmp/omniEngine.lock.'+str(USER)
now=datetime.now()
sys.argv.pop(0)
lastStatusUpdateTime=None

if os.path.isfile(lockFile):
  #open the lock file to read pid and timestamp
  file=open(lockFile,'r')
  data=file.readline()
  file.close()
  pid=data.split(',')[0]
  timestamp=data.split(',')[1]
  #check if the pid is still running
  if os.path.exists("/proc/"+str(pid)):
    print "Exiting: OmniEngine already running with pid:", pid, "  Last parse started at ", timestamp
  else:
    print "Stale OmniEngine found, no running pid:", pid, " Process last started at: ", timestamp
    print "Removing lock file and waiting for restart"
    os.remove(lockFile)
  #exit program and wait for next run
  exit(1)
else:
  #start/create our lock file
  file = open(lockFile, "w")
  file.write(str(os.getpid())+','+str(now))
  file.close()

  #set our debug level, all outputs will be controlled by this
  try:
    if len(sys.argv) == 1:
      #use debug level from cmd line
      debuglevel = int(sys.argv[0])
    else:
      #invlid cmdline options use default value
      debuglevel = 5
  except:
    #invlid cmdline options use default value
    debuglevel = 5

  setdebug(debuglevel)

  printdebug(("Processing started at",now), 0)

  #block with first MP transaction

  # first known testnet transaction as determined through OP_RETURN containing omni
  # txid: 6e379ffd4d82698c69cad21dee10062ec832e552f9690dfcde69e68c0b5dcf56
  # blockhash: 03eed1ea0f2633064cbf479b1562d03496baa29d798debf9170f19c2c3465696
  # block: 1843740

  # first known mainnet transaction
  # txid: 09151f29be5f2c93e022dccd855183cd1b03b632eaa9105e071d85f151cbab87
  # blockhash: 53399d5b77ff52d1f672be9b482598fd66f35e9f302d2c53f24b5bb5d089b7cc
  # block: 2093636

  if config.TESTNET:
    firstMPtxBlock=1843740
  else:
    firstMPtxBlock=2093636

  #get last known block processed from db
  currentBlock=dbSelect("select max(blocknumber) from blocks", None)[0][0]
  printdebug(("Current block is ",currentBlock), 0)
  if currentBlock is not None:
    currentBlock=currentBlock+1
  else:
    currentBlock=firstMPtxBlock

  #Find most recent block mastercore has available
  endBlock=getinfo()['result']['blocks']

  #reorg protection/check go back 10 blocks from where we last parsed
  checkBlock=max(currentBlock-10,firstMPtxBlock)
  while checkBlock < currentBlock:
    hash = getblockhash(checkBlock)['result']
    dbhash=dbSelect('select blockhash from blocks where blocknumber=%s',[checkBlock])[0][0]
    if hash == dbhash:
      #everything looks good, go to next block
      checkBlock+=1
    else:
      #reorg took place
      try:
        print "Reorg detected, Attempting roll back to ",checkBlock-1
        reorgRollback(checkBlock-1)
        currentBlock=checkBlock
        dbCommit()
        break
      except Exception,e:
        #Catch any issues and stop processing. Try to undo any incomplete changes
        print "Problem with ", e
        if dbRollback():
          print "Database rolledback, last successful block", (currentBlock -1)
        else:
          print "Problem rolling database back, check block data for", currentBlock
        exit(1)


  if currentBlock > endBlock:
    printdebug("Already up to date",0)
    updateRan=False
  else:
    rExpireAllBalBTC()
    updateRan=True

  #get highest TxDBSerialNum (number of rows in the Transactions table)
  #78681427 ltc tx's before block 2093636
  TxDBSerialNum=dbSelect('select coalesce(max(txdbserialnum), 78681427) from transactions')[0][0]+1

  #main loop, process new blocks
  while currentBlock <= endBlock:
    try:
      hash = getblockhash(currentBlock)['result']
      block_data = getblock(hash)
      height = block_data['result']['height']

      #don't waste resources looking for MP transactions before the first one occurred
      if height >= firstMPtxBlock:
        block_data_MP = listblocktransactions_MP(height)
      else:
        block_data_MP = {"error": None, "id": None, "result": []}

      #status update
      if lastStatusUpdateTime == None:
        printdebug(("Block",height,"of",endBlock),1)
        lastStatusUpdateTime=datetime.now()
      else:
        statusUpdateTime=datetime.now()
        timeDelta = statusUpdateTime - lastStatusUpdateTime
        blocksLeft = endBlock - currentBlock
        projectedTime = str(timedelta(microseconds=timeDelta.microseconds * blocksLeft))
        printdebug(("Block",height,"of",endBlock, "(took", timeDelta.microseconds, "microseconds, blocks left:", blocksLeft, ", eta", projectedTime,")"),1)
        lastStatusUpdateTime=statusUpdateTime

      #Process Litecoin Transacations
      Protocol="Litecoin"

      #Find number of tx's in block
      txcount=len(block_data['result']['tx'])
      printdebug((txcount,"LTC tx"), 1)

      #Write the blocks table row
      insertBlock(block_data, Protocol, height, txcount)

      #check for pendingtx's to cleanup
      checkPending(block_data['result']['tx'])

      #count position in block
      x=1
      for tx in block_data['result']['tx']:
        #rawtx=getrawtransaction(tx)
        #serial=insertTx(rawtx, Protocol, height, x, TxDBSerialNum)
        #serial=insertTx(rawtx, Protocol, height, x)
        #insertTxAddr(rawtx, Protocol, serial, currentBlock)

        #increment the number of transactions
        TxDBSerialNum+=1
        #increment tx sequence number in block
        x+=1

      #Process Mastercoin Transacations (if any)
      Protocol="Omni"

      #Find number of msc tx
      y=len(block_data_MP['result'])
      if y != 0:
        printdebug((y,"OMNI tx"), 1)

      #count position in block
      x=1
      #MP tx processing
      for tx in block_data_MP['result']:
        rawtx=gettransaction_MP(tx)

        #Process the bare tx and insert it into the db
        #TxDBSerialNum can be specified for explit insert or left out to auto assign from next value in db
        serial=insertTx(rawtx, Protocol, height, x, TxDBSerialNum)
        #serial=insertTx(rawtx, Protocol, height, x)

        #Process all the addresses in the tx and insert them into db
        #This also calls the functions that update the DEx, SmartProperty and AddressBalance tables
        insertTxAddr(rawtx, Protocol, serial, currentBlock)

        #increment the number of transactions
        TxDBSerialNum+=1

        #increment tx sequence number in block
        x+=1

      #Clean up any offers/crowdsales that expired in this block
      #Run these after we processes the tx's in the block as tx in the current block would be valid
      #expire the current active offers if block time has passed
      expireAccepts(height)
      #check active crowdsales and update json if the endtime has passed (based on block time)
      expireCrowdsales(block_data['result']['time'], Protocol)

      #Also make sure we update the json data in SmartProperties table used by exchange view
      updateProperty(1,"Omni")
      updateProperty(2,"Omni")
      #check any pending activations
      checkPendingActivations()
      #make sure we store the last serialnumber used
      dbExecute("select setval('transactions_txdbserialnum_seq', %s)", [TxDBSerialNum-1])
      #write db changes for entire block
      dbCommit()

    except Exception,e:
      #Catch any issues and stop processing. Try to undo any incomplete changes
      print "Problem with ", e
      if dbRollback():
        print "Database rolledback, last successful block", (currentBlock -1)
      else:
        print "Problem rolling database back, check block data for", currentBlock
      os.remove(lockFile)
      exit(1)

    try:
      #Also make sure we update the txstats data per block
      printdebug("TxStats update started",0)
      updateTxStats()
      dbCommit()
      printdebug("TxStats updated",0)
    except:
      pass

    #increment/process next block if everything went clean
    currentBlock += 1

  #/while loop.  Finished processing all current blocks.
  try:
    #Also make sure we update the json data in SmartProperties
    updateProperty(0,"Litecoin")
    dbCommit()
  except:
    pass

  try:
    #Try to update consensushash of last block
    updateConsensusHash()
    dbCommit()
  except:
    pass

  if config.TESTNET and updateRan:
    try:
      #Reset omni balances on testnet to account for moneyman transactions
      resetbalances_MP([1,2])
      dbCommit()
    except:
      pass

  #check/add/update and pending tx in the database
  try:
    updateAddPending()
    dbCommit()
    printdebug("Pending List updated",0)
  except Exception,e:
    #Catch any issues and stop processing. Try to undo any incomplete changes
    print "Problem updating pending ", e
    if dbRollback():
      print "Database rolledback"
    else:
      print "Problem rolling database back, check pending data"
    os.remove(lockFile)
    exit(1)

  try:
    #Also make sure we update the last run
    updateLastRun()
    dbCommit()
  except:
    pass

  #remove the lock file and let ourself finish
  os.remove(lockFile)

#/end else for lock file
