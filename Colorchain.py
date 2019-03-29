import requests
import json
import encodings
import MySQLdb
import time
import datetime
import cointools

dbhost=''
dbuser=''
dbpassword=''
dbname=''

blocksperfile=100

def download_block(x):
    a=requests.get("https://blockchain.info/block-height/"+str(x)+"?format=json")

    if(a.status_code==200):
        b=json.loads(str(a.content))

        f=0
        for x in b['blocks']:
            if(x['main_chain']):
                #is main chain block, interpret

                return x

def saveblock(x):

    whichdb='blocks'+str(int(x/blocksperfile))+'.txt' 

    if(x==0):
        f=open(whichdb,'w') 
    else:
        f=open(whichdb,'a+')  #opens for appending and reading only

    b=download_block(x)  #already returned as JSON object
    b2=json.dumps(b)  #non-json
    print str(x) + "    "+str(len(b2))
    #use 8 byte leader string to say length of subsequent block in file
    #must always be 8 bytes
    leadermax=8
    leader=len(b2)
    leaderstring=str(leader)
    p=leadermax-len(leaderstring)
    a=0
    while a<p:
        leaderstring='0'+leaderstring
        a=a+1

    f.write(leaderstring)
    f.write(b2)
    #print len(leaderstring) #to make sure its always 8

    f.close()

def saveblocks(startx,endx):
    x=startx
    global lastblockdb
    while(x<endx+1):
        #print x
        r=saveblock(x)

        if(r==False):
            x=endx+1

        x=x+1

def loadblockfile(x):  #from locally saved blocks in txt format
    global blocksdb, blockfile
    blocksdb=[]
    if(False):
        return -1
    else:
        blockfile=open("blocks"+str(x)+".txt")

        #open blocks sequentially
        go=True
        while go:
            leader=blockfile.read(8)  #not referenced to 8 seen elsewhere
            if(leader==''): #at end of file
                go=False
            else:
                leader=int(leader)
                blk=blockfile.read(leader)
                blk=json.loads(blk)
                blocksdb.append(blk)


        #stuff=json.load(blockfile)
        #for x in stuff:
        #    blocksdb.append(x)
    return blockfile



def transactions_in_block(blockobject):
    global a
    transactions=blockobject['tx']
    newtransactions=0
    newaddresses=0
    returns=[]
    lt=len(transactions)
    lk=1

    for trans in transactions:
        newtransactions=newtransactions+1
        #handle inputs
        inps=trans['inputs']
        for ins in inps:
            if(len(ins)>0):
                if blockobject['height']<129878:
                    addr=ins['prev_out']['addr']
                    amt=int(ins['prev_out']['value'])
                  #  print addr +"  "+str(amt)
                else:
                    addr=ins['prev_out']['addr']
                    amt=ins['prev_out']['value']
                   # print addr+"  "+str(amt)

                a=read_address(addr)
                if len(a)>0: #already exists in db:
                    newbtc=a[0][1]-amt
                    ntrans=a[0][2]+1
                    totrcv=a[0][3]#-amt
                    update_address(addr,newbtc,ntrans,totrcv)
                else:
                    add_address(addr,-1*amt,1,0)
                    newaddresses=newaddresses+1

        outs=trans['out']
        for out in outs:
            try:
                addr=out['addr']
                amt=out['value']
            except:
                addr='invalid'
                amt=out['value']
           # print addr+"  "+str(amt)

            if len(addr)>0:

                a=read_address(addr)
                if len(a)>0: #already exists in db:
                    newbtc=a[0][1]+amt
                    ntrans=a[0][2]+1
                    totrcv=a[0][3]+amt
                    update_address(addr,newbtc,ntrans,totrcv)
                else:
                    add_address(addr,amt,1,amt)
                    newaddresses=newaddresses+1


        #update overview as done with this tx

        f="UPDATE OVERVIEW SET LAST_TX_HASH='"+str(trans['hash'])+"';"
        print trans['hash']+"  "+str(lk)+" / "+str(lt)
        lk=lk+1
        curs=db.cursor()
        curs.execute(f)

 #   db.commit()
   # db.close()

    returns.append(newaddresses)
    returns.append(newtransactions)
    return returns

def purge_empty():
    db=connect_to_db()
    f="DELETE FROM Addresses WHERE BTC='0';"
    curs=db.cursor()
    curs.execute(f)

    db.commit()
    db.close()

def block(x, local):
    global db
    db=connect_to_db()
    answer=-1
    if not local:
        a=download_block(x)
        print "Block Downloaded"
    else:
        v=x%blocksperfile
        w=int(x/blocksperfile)
        loadblockfile(w)
        a=blocksdb[v]


    t=a['time']
    j=datetime.datetime.fromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S')

    overviewdata=transactions_in_block(a)

    curs=db.cursor()

    f="SELECT * FROM OVERVIEW;"
    curs.execute(f)
    c=curs.fetchall()

   #update overview data
    f="UPDATE OVERVIEW SET LASTBLOCK='"+str(x)+"'"
    f=f+",N_TRANSACTIONS='"+str(int(c[0][1])+overviewdata[1])+"',N_ADDRESSES='"
    f=f+str(int(c[0][2])+overviewdata[0])+"',TIME='"+j+"', LAST_TX_HASH='donewithblock';"



    curs.execute(f)
    db.commit()

    #purge_empty()

    db.close()
    print "BLOCK: "+str(c[0][0])+"   N_ADDRESSES: "+str(c[0][2])+"  N_TRANSACTIONS: "+str(c[0][1])+"   "+str(j)

    j=check()
    if j==5000000000:
        return True
    else:
        return False

def check():
    global c
    db=connect_to_db()
    a=lastblock()
    curs=db.cursor()
    f='SELECT BTC FROM Addresses;'
    curs.execute(f)
    c=curs.fetchall()
    b=0
    for x in c:
        b=b+x[0]
    return b/(a+1)


def lastblock():
    db=connect_to_db()
    curs=db.cursor()
    f="SELECT * FROM OVERVIEW;"
    curs.execute(f)
    a=curs.fetchall()
    return int(a[0][0])

def blocks(start,end):  #inclusive
    a=start
    g=lastblock()
    if a>g:
        while a<end+1:
            #t=(datetime.datetime.fromtimestamp(int(k)).strftime('%Y-%m-%d %H:%M:%S'))
            #print "BLOCK: "+str(a)#+"       "+str(t)
            j=block(a,True)

            if not j:
                a=end+1
                print "Check not correct"

            a=a+1
    else:
        print "dont incorporate blocks redundantly!"

    check()

def connect_to_db():
    db=MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpassword,db=dbname)
    return db

def init():
    global db
    db=connect_to_db()
    print "LAST BLOCK: "+str(lastblock())


def read_address(the_address):
    curs=db.cursor()
    if len(the_address)<50:
        f="SELECT * FROM Addresses WHERE ADDRESS='"+the_address+"'"+";"
        curs.execute(f)
    a=curs.fetchall()
    return a

def add_address(the_address,btc,ntransactions,totalreceived):
    curs=db.cursor()
    if len(the_address)<50 and len(str(btc))<40 and len(str(ntransactions))<50 and len(str(totalreceived))<50:
        f="INSERT INTO Addresses (ADDRESS,BTC,N_TRANSACTIONS,TOTAL_RECEIVED) VALUES ('"+the_address+"','"+str(btc)
        f=f+"','"+str(ntransactions)+"','"+str(totalreceived)+"');"
        curs.execute(f)
        db.commit()

def update_address(the_address,btc,ntransactions,totalreceived):

   curs=db.cursor()

    #update existing entry
   if len(the_address)<50 and len(str(btc))<40 and len(str(ntransactions))<50 and len(str(totalreceived))<50:
        f="UPDATE Addresses SET BTC='"+str(btc)+"'"
        f=f+",N_TRANSACTIONS='"+str(ntransactions)+"',TOTAL_RECEIVED='"+str(totalreceived)+"'"
        f=f+"WHERE ADDRESS='"+the_address+"';"
        curs.execute(f)
        db.commit()


def delete_all_addresses():
    db=connect_to_db()
    curs=db.cursor()
    f="DELETE FROM Addresses;"
    curs.execute(f)
    db.commit()
    db.close()


#COLOR COIN

def add_color_coin(name, sourceaddress, referenceaddress):
    #add table to 'Addresses in DB'
    db=connect_to_db()
    curs=db.cursor()
    f="ALTER TABLE Addresses"
    f=f+" ADD "+str(name)+" BIGINT;"

    curs.execute(f)

    f="INSERT INTO COINCOLORS VALUES ('"
    f=f+name+"','"+sourceaddress+"','"+referenceaddress+"');"
    curs.execute(f)
    db.commit() 
    db.close()

def get_color_info(color):
    #returns name, source, reference
    db=connect_to_db()
    curs=db.cursor()
    f="SELECT * FROM COINCOLORS WHERE NAME='"+color+"';"
    curs.execute(f)
    a=curs.fetchall()
    db.close()
    if len(a)==0:
        return 0
    return a[0]

def info_to_addr(info):
    a=info.encode('hex')
    print len(a)
    return cointools.hex_to_address(a)

def send_color_coin(fromaddr,toaddr,amt,colorname,fromsecretexponent):
    #sends X satoshi to destination address (toaddr)
    #sends 1 satoshi to ColoredCoin Reference Address
    #sends HEX INFO Base 58 encoded into address to indicate TX HASH of previous address

    db=connect_to_db()
    curs=db.cursor()
    f="SELECT * FROM COINCOLORS WHERE NAME='"+colorname+"';"
    curs.execute(f)
    a=curs.fetchall()
    referenceaddress=a[0][2]
    sourceaddress=a[0][1]

    to=[]
    to.append(toaddr)#destination first
    to.append(referenceaddress)# reference address second
    #previous tx  first half of identifying hash  is THIRD
    #seoncd half of previous hash if FOURTH
    prevtxhash='f85e3c10f02e11822bf6964456fabd126ef485f387246ecb94213ef577ca0a1b'   #GET THIS SOMEHOW
    firsthalfhash=prevtxhash[0:len(prevtxhash)/2]
    secondhalfhash=prevtxhash[len(prevtxhash)/2:len(prevtxhash)]
    to.append(cointools.hex_to_address(firsthalfhash))
    to.append(cointools.hex_to_address(secondhalfhash))

    outputs=[]
    p=amt*0.00000001
    outputs.append(p)
    outputs.append(0.00000001)
    outputs.append(0.00000001)
    outputs.append(0.00000001)
    fee=cointools.standard_fee

    cointools.send_many(fromaddr,outputs,to,fee,0,0,fromsecretexponent)

    db.close()
    return a


def read_colored_in_block(blockobject, color):
    transactions=blockobject['tx']

    db=connect_to_db()
    curs=db.cursor()
    f="SELECT * FROM COINCOLORS WHERE NAME='"+color+"';"
    curs.execute(f)
    a=curs.fetchall()
    #print a

    ctransactions=[]
    if len(a)>0:
        reference=a[0][2]
        source=a[0][1]
        for x in transactions:
            colored=False
            c=x['out']
            for y in c:
                try:
                    if y['addr']==reference:
                        colored=True
                except:
                    print "invalid recipient detected somewhere in block"

            if colored:
                ctransactions.append(x)

    db.close()
    return ctransactions

def read_color_address(the_address, color):
    global aa
    curs=db.cursor()
    a=[]
    if len(the_address)<50:
        f="SELECT "+color+" FROM Addresses WHERE ADDRESS='"+the_address+"'"+";"
        #try:
        curs.execute(f)
        a=curs.fetchall()
        #except:
            #print "Coin Not Found"

    if len(a)==0:
        a=[[0]]


    g=get_color_info(color)
    if not g==0: #coin is valid
        if g[1]==the_address: 
            print "SOURCE ADDRESS"
            a=99999999999999
            return a
        else:
            return a[0][0]
    else:
        print "Invalid Color"
        return -1
    aa=a


def add_color_address(the_address,color,amt):  #input amt as integer, goes in DB as integer
    curs=db.cursor()
    #amt=amt*0.00000001

    f="INSERT INTO Addresses (ADDRESS,"+color+") VALUES ('"+the_address+"','"+str(amt)+"');"
    curs.execute(f)
    db.commit()

def update_color_address(the_address,color,amt):
   curs=db.cursor()
   #amt=amt*0.00000001
    #update existing entry

   f="UPDATE Addresses SET "+color+"='"+str(amt)+"' "
   f=f+"WHERE ADDRESS='"+the_address+"';"
   curs.execute(f)
   db.commit()



def color_block(blockobject,color): #process colored transactions in block X    
    db=connect_to_db()
    ctrans=read_colored_in_block(blockobject,color)
    #need to check each transaction for legitimacy
    global outvalues, outputs, inpaddr, totalin
    colorinfo=get_color_info(color) 

    for x in ctrans:
        #check sender's value
        totalin=0
        outputs=[]
        outvalues=[]

        #only 1 input allowed for colored coin parsing purposes, can have others for tx fees
        y=x['inputs'][0]
                    #for y in x['inputs']:
        inpaddr=y['prev_out']['addr']
        r=read_color_address(inpaddr,color)
        print str(inpaddr)+"  "+str(r)
        totalin=totalin+r


        for y in x['out']:
            if not y['addr']==colorinfo[2] and not y['addr']==inpaddr: #DONT COUNT values sent to reference
                outputs.append(y['addr'])
                outvalues.append(y['value'])

        sum=0
        for y in outvalues:
            sum=sum+y

        if sum<=totalin:
            #transaction is OK

            a=0
            while a<len(outputs):

                g=read_color_address(outputs[a],color)
                if g==0:
                    add_color_address(outputs[a],color,outvalues[a])
                else:
                    update_color_address(outputs[a],color,g+outvalues[a])

                a=a+1

            #only 1 input per transaction allowed, first
            update_color_address(inpaddr,color,totalin-sum)
    db.close()   

def color_blocks(start,end,color):  #inclusive, NOT THE only way to search
    #probably not very efficient anyway
    b=start
    while b<end+1:
        a=download_block(b)
        color_block(b,color)
        print "BLOCK: "+str(b)+" CHECKED FOR COLOR: "+str(color)
        b=b+1

def download_tx(txid):
    #may be vulnerable to malleability?
    g='https://blockchain.info/tx-index/'+str(txid)+'?format=json'
    a=requests.get(g)
    if a.status_code==200:
        b=a.content
        c=json.loads(str(b))
    if c['double_spend']==False:
        return c
    else:
        print "tx double spends"


#def search_reference_history(referenceaddress):  #searches history of reference id 

def check_address(addr):
    a='https://blockchain.info/q/addressbalance/'+str(addr)
    b=requests.get(a)
    if b.status_code==200:
        c=b.content

    #return c
    curs=db.cursor()
    f="SELECT * FROM Addresses WHERE Address='"+str(addr)+"';"
    curs.execute(f)
    g=curs.fetchall()
    btc=g[0][1]

    if btc==c:
        p=0
    else:
        print str(addr)+"   "+str(c)+"   "+str(btc)

def check_all_addresses():
    curs=db.cursor()
    f="SELECT * FROM Addresses;"
    curs.execute(f)
    g=curs.fetchall()
    a=[]
    for x in g:
        a.append(x[0])
    return a



def process_tx(blockn,fromtxhash):
    blockobject=download_block(blockn)
    blockobjectt=blockobject['tx']
    for trans in blockobjectt:
        newtransactions=0
        newaddresses=0
        returns=[]
        if trans['hash']==fromtxhash:
            #process that tx

            newtransactions=newtransactions+1
            #handle inputs
            inps=trans['inputs']
            for ins in inps:
                if(len(ins)>0):
                    if blockobject['height']<129878:
                        addr=ins['prev_out']['addr']
                        amt=int(ins['prev_out']['value'])
                      #  print addr +"  "+str(amt)
                    else:
                        addr=ins['prev_out']['addr']
                        amt=ins['prev_out']['value']
                       # print addr+"  "+str(amt)

                    a=read_address(addr)
                    if len(a)>0: #already exists in db:
                        newbtc=a[0][1]-amt
                        ntrans=a[0][2]+1
                        totrcv=a[0][3]#-amt
                        update_address(addr,newbtc,ntrans,totrcv)
                    else:
                        add_address(addr,-1*amt,1,0)
                        newaddresses=newaddresses+1

            outs=trans['out']
            for out in outs:
                try:
                    addr=out['addr']
                    amt=out['value']
                except:
                    addr='invalid'
                    amt=out['value']
               # print addr+"  "+str(amt)

                if len(addr)>0:

                    a=read_address(addr)
                    if len(a)>0: #already exists in db:
                        newbtc=a[0][1]+amt
                        ntrans=a[0][2]+1
                        totrcv=a[0][3]+amt
                        update_address(addr,newbtc,ntrans,totrcv)
                    else:
                        add_address(addr,amt,1,amt)
                        newaddresses=newaddresses+1






init()

db=connect_to_db()
c=db.cursor()
#f='SELECT * FROM Addresses;'
#f="""INSERT INTO Addresses(Address,BTC) VALUES (barisser,132);""" 
#c.execute(f)
#db.commit()
#db.close()

#for row in c.fetchall():
    #print row
