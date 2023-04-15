import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
import socket
import threading
import sys

# --- Shop IDs for testing 
ShellShop1_Id = "C38D220929150"
ShellShop2_Id ="C38D220929154"
FurnishingShop1_Id = "C38D220929151"
FurnishingShop2_Id = "C38D220929153"

# --- constants ---

HOST = '192.168.1.100'
PORT = 10009

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()


class ShopTagStatus:
    def __init__(self, documentId, tagId, readerSerialNo, inTime, shopId, status):
        self.documentId = documentId
        self.id = tagId
        self.readerSerialNo = readerSerialNo
        self.inTime = inTime
        self.outTime = None
        self.shopId = shopId,
        self.status = status,


class Reader:
    def __init__(self, serialNo, shopId):
        self.serialNo = serialNo
        self.shopId = shopId


def log(str):
    log = {'log': str, 'time': datetime.now(), }
    db.collection("Log").set(log)


def handleFirebase(receivedSerialNumber, receivedTagId):
    print('Handle Firebase has received data. serialNumber: ' + receivedSerialNumber + ' tagId: ' + receivedTagId)
    readerListData = db.collection("ReaderList").where('SerialNumber', '==', receivedSerialNumber).get()
    readerItemData = readerListData[0].to_dict()
    reader = Reader(readerItemData['SerialNumber'], readerItemData['ShopId'])

    tagListDataList = db.collection("ShopTagStatus").where('tagId', '==', receivedTagId).where('isCompleted', '==',
                                                                                               'false').get()
    print("type is ", type(tagListDataList))
    if len(tagListDataList) == 0:
        print("First Entry @", datetime.utcnow())
        newTagArrived = {'tagId': receivedTagId, 'readerSerialNo': receivedSerialNumber, 'inTime': datetime.utcnow(),
                         'shopId': reader.shopId,
                         'status': 'in', 'isCompleted': 'false'}
        documentId = receivedTagId + str(datetime.now())
        db.collection('ShopTagStatus').document(documentId).set(newTagArrived)
        # log("TagId: " + receivedTagId + ' arrived. Making an Entry to database. data: ' + newTagArrived)
    else:
        recordLen = len(tagListDataList)
        NoRecord = 0
        for item in tagListDataList:
          print("Existing Record @", datetime.utcnow())
          #tagItemData = item.to_dict
          print("Tag ID",item.get('tagId'))
          shopTagStatus = ShopTagStatus(item.id, item.get('tagId'),item.get('readerSerialNo'),
                                      item.get('inTime'), item.get('shopId'), item.get('status'))
          if shopTagStatus.readerSerialNo == receivedSerialNumber:
              if shopTagStatus.status[0] == 'in':
                timeDiff = (datetime.utcnow() - datetime.utcfromtimestamp(shopTagStatus.inTime.timestamp())).seconds
                # timeDiff = 16*60
                # if same tag is detected after 15 minute then update out time and mark out from this shop
                if timeDiff > 1 * 30:
                    print("Marking out @", datetime.utcnow())
                    updateShopTagStatus = {'outTime': datetime.utcnow(), 'status': 'out'}
                    db.collection("ShopTagStatus").document(shopTagStatus.documentId).update(updateShopTagStatus)
                else:
                    print("Reentry @", datetime.utcnow())
                    # if same tag is detected in less than a minute update in time to reset counter
                    updateShopTagStatus = {'inTime': datetime.utcnow()}
                    db.collection("ShopTagStatus").document(shopTagStatus.documentId).update(updateShopTagStatus)
                    # log("TagId: " + receivedTagId + ' has again arrived. TimeDiff is ' + timeDiff + '. Marking it as
                    # out: ')
          else:
              NoRecord = NoRecord + 1
        if NoRecord == recordLen:
		  #We did not find entry in our db. Its a new entry in other shop make this entry
          print("Making new entry for another shop @", datetime.utcnow())
          newTagArrived = {'tagId': receivedTagId, 'readerSerialNo': receivedSerialNumber, 'inTime': datetime.utcnow(),
                         'shopId': reader.shopId,
                         'status': 'in', 'isCompleted': 'false'}
          documentId = receivedTagId + str(datetime.now())
          db.collection('ShopTagStatus').document(documentId).set(newTagArrived)


def handle_client(conn, addr):
    try:
        while True:
            received_data = str(conn.recv(128))
            res1 = received_data.split('readsn=', 1)
            res2 = received_data.split('id=', 1)

            serialNumber = res1[1][0:13]
            tagId = res2[1][0:4]
            handleFirebase(serialNumber, tagId)

    except BrokenPipeError:
        print('[DEBUG] addr:', addr, 'Connection closed by client?')
    except Exception as ex:
        print('[DEBUG] addr:', addr, 'Exception:', ex, )
    finally:
        conn.close()


handleFirebase(ShellShop2_Id, "A232132")
try:
    print()
    print('[DEBUG] create socket')
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print('[DEBUG] bind:', (HOST, PORT))
    s.bind((HOST, PORT))
    print('[DEBUG] listen')
    s.listen(1)
    while True:
        print('[DEBUG] accept ... waiting')
        conn, addr = s.accept()
        print('[DEBUG] addr:', addr)
        t = threading.Thread(target=handle_client, args=(conn, addr))
        t.start()
except Exception as ex:
    print(ex)
except KeyboardInterrupt as ex:
    print(ex)
except:
    print(sys.exc_info())
finally:
    print('[DEBUG] close socket')
    s.close()
