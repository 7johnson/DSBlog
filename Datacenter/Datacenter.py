'''
Created on 29 Apr 2016

@author: Johnny
'''
import LogItem
import Message

class Datacenter:
    
    def __init__(self,localDCNum,totalDCNum):
        Dictionary={}
        
        TT=[]
        for i in range(totalDCNum):
            TT.append([])
        for i in range(totalDCNum):        
            for j in range(totalDCNum):
                TT[i].append(0)
                
        Log=[]
        for i in range(totalDCNum):
            Log.append([])
        
        TTmin=[]
        for i in range(totalDCNum):
            TTmin.append(0)
            
        self.Log=Log;
        self.TT=TT;
        self.Dictionary=Dictionary
        self.localDCNum=localDCNum
        self.totalDCNum=totalDCNum
        self.curTimestamp=0
        self.TTmin=TTmin
        
    def receive(self,message):
        
        self.updateTT(message.TT)
        self.updateLog(message.Log)
        
    def updateTT(self,recTT):
        
        for j in range(len(self.TT)):
            maxTemp=0
            for i in range(len(self.TT)):
                self.TT[i][j]=max(self.TT[i][j],recTT[i][j])
                maxTemp=max(self.TT[i][j],maxTemp)
            else:
                self.TT[self.localDCNum][j]=maxTemp
        
        self.TTmin=[];
        
        for j in range(len(self.TT)):
            minTemp=self.TT[self.localDCNum][j]
            for i in range(len(self.TT)):
                minTemp=min(self.TT[i][j],minTemp)
            else:
                self.TTmin[j]=minTemp
            
    
    def updateLog(self,recLog):            
        
        for i in range(len(recLog)):
            for j in range(len(recLog[i])):
                if recLog[i][j].timestamp>self.TTmin[i]:
                    self.Log[i].append(recLog[i][j])
                    self.Dictionary[recLog[i][j].orderNum] = recLog[i][j].post       
                    if recLog[i][j].timestamp>self.curTimestamp:
                        self.curTimestamp=recLog[i][j].timestamp+1
                        
        for i in range(len(self.Log)):
            for j in range(len(self.Log[i])):
                if self.Log[i][j].timestamp<=self.TTmin[i]:
                    del self.Log[i][j]
    
    def onSync(self,destDCNum):
        
        sentLog=[]
        for i in range(len(self.Log)):
            sentLog.append([])
        
        
        for i in range(len(self.Log)):
            for j in range(len(self.Log[i])):
                if self.Log[i][j].timestamp>self.TT[destDCNum][i]:
                    sentLog[i].append(self.Log[i][j])
                    
        sentMessage=Message(sentLog,self.TT)
    
    def onPost(self,post):
        
        self.curTimestamp+=1
        orderNum=self.curTimestamp+self.localDCNum*0.1
        
        item=LogItem(orderNum,self.curTimestamp,post)
        self.Log[self.localDCNum].append(item)
        
        self.Dictionary[item.orderNum] = item.post
        
        self.TT[self.localDCNum][self.localDCNum]=self.curTimestamp       
    
    def onLookup(self):
        
        
        
                       