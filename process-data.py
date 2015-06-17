#!/usr/bin/python
#encoding: utf-8
import csv
import glob
import datetime
import sys
import os

class BatchProcessCSV:
    def __init__(self,inputfile="in/data.csv",outputfile="out/data.csv"):
        self.inputfile=inputfile
        self.outputfile=outputfile

    def doBatchAction(self):
        startTime=datetime.datetime.now()
        print("start...")
        self.readcsv2csv(self.inputfile, self.outputfile)
        print("Running.........................\n")
        endTime=datetime.datetime.now()
        print("finished：%fs"%(endTime-startTime).seconds)

    def readcsv2csv(self,inputfile,outputfile):
        map = {}
        with open(inputfile, 'rb') as csvfile:
            column=csvfile.readline().strip().split(",")
            print column;
            reader = csv.reader(csvfile)
            for row in reader:
                lenth=len(row)
                if lenth>5:
                    value_E = row[column.index("E")]
                    if value_E:
                        value_E = value_E.strip()
                    value_F = row[column.index("F")]
                    if value_F:
                        value_F = value_F.strip()
                    if value_E and value_F:
                        map[value_E] = value_F

        with open(inputfile, 'rb') as csvfile:
            o=open(outputfile,"wb")
            writer=csv.writer(o)
            column=csvfile.readline().strip().split(",")
            print column;
            writer.writerow(["A","B","C","D","E","F"]);
            reader = csv.reader(csvfile)

            for row in reader:
                lenth=len(row)
                if lenth>5:
                    value_A = row[column.index("A")]
                    value = False
                    if value_A:
                        value_A = value_A.strip()
                        value = map.get(value_A)
                    if value != False and value != "" and value != None:
                        writer.writerow([row[column.index("A")],value,row[column.index("C")],row[column.index("D")],row[column.index("E")],row[column.index("F")]])
                    else:
                        writer.writerow([row[column.index("A")],row[column.index("B")],row[column.index("C")],row[column.index("D")],row[column.index("E")],row[column.index("F")]])

if __name__=="__main__":
    csvProcess=BatchProcessCSV("/home/qinshulei/projects/data-process/in/data.csv","/home/qinshulei/projects/data-process/out/data.csv")
    csvProcess.doBatchAction()
