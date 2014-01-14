'''
Created on 9 Jan 2014

@author: Andrew
'''
from java.sql import *
from java.lang import Class
from java.lang import Math
from com.mysql.jdbc import Driver

DB_URL = "jdbc:mysql://isis:3306/devcore"
#DB_URL = "jdbc:mysql://isis:3306/quartz"
#DB_URL = "jdbc:mysql://isis:3306/perf2core"
DB_USER = "root"
DB_PASSWD = "pact"

def getConnection():
    # load driver
    driverName = "com.mysql.jdbc.Driver"
    Class.forName(driverName)
    # get connection to database
    return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWD)

def execute(QUERY, trace=0):
    if trace:
        print QUERY
    try:
        connection = getConnection()
        statement = connection.createStatement()
        resultSet = statement.executeQuery(QUERY)
        columnCount = resultSet.getMetaData().getColumnCount()
        results = ()
        while resultSet.next():
            result = ()
            for column in range(columnCount):
                result+= (resultSet.getString(1+column),)
            results += (result,)
        connection.close();
        statement.close();
        return results
    except  SQLException, e: # if table does not exist, create a new table
        print "Exception1 -", e
        f.write(QUERY + "\n" + date + str(e) + "\n")
        connection.close();
        statement.close();


#print execute("show tables")


def analyseDistinctValues(distincts):
    reverseCount = {}
    example = {}
    for distinct in distincts:
        distinctVal = eval(distinct[1])
        try:
            reverseCount[distinctVal]
        except:
            reverseCount[distinctVal]=0
            example[distinctVal]=distinct[0]
        reverseCount[distinctVal]+=1
        #print reverseCount
    return reverseCount,example
        

def prepare(sample):
    rows = ()
    for frequency in sorted(sample):
        rows += ((frequency, sample[frequency], 0),)
        
    return rows

def log2up(n):
    return round(0.4999+Math.log(n) / Math.log(2),0)

def condense(sample):
    for i in range(len(sample)):
        bits = log2up(sample[i][1])
        sample[i] = (sample[i][1], sample[i][0], bits)
    return sample
    
    
def combine(sample):
    a = sample[0]
    b = sample[1]
#    c =(a[0]*a[1]+b[0]*b[1], a[1]+b[1], 1+(a[2]*a[1] + b[2]*b[1])/(a[1]+b[1]))
    c =(a[0]*a[1]+b[0]*b[1], 1, 1+(a[2]*a[0] + b[2]*b[0])/(a[0]+b[0]))
    return [c,]+sample[2:]

def collapse(sample, trace=0):
    if trace:
        print sample
    if len(sample)>1:
        return collapse(sorted(combine(sample)))
    else:
        return sample



def analyseTable(tablename, trace=0):
    tableDict = {}
    tableDict["tableName"]=tablename
    columns = execute("describe "+tablename)
    tableDict["tableColumnCount"]=len(columns)
    count = eval(execute("select count(*) from "+tablename)[0][0])
    tableDict["tableRowCount"]=count
    tableDict["tableColumns"]={}
    for column in columns:
        columnDict = {}
        columnDict["columnName"] = column[0]
        columnType = "data"
        columnQualifier = ""
        if trace:
            print column[0]
        query = ("select count(distinct "+column[0]+") from "+tablename)
        distinctCount = eval(execute(query)[0][0])
            
        #print distinctCount
        if distinctCount == count:
            if trace:
                print "Each value is distinct"
            columnType = "unique"
            columnDict["information"]=log2up(distinctCount)
        else:
            distincts = execute("select distinct "+column[0]+", count(*) from "+tablename+" group by "+column[0])
            #print distincts
            reverseCounts, examples = analyseDistinctValues(distincts)
            #print reverseCounts
            celltotal = 0
            cells = 0
            for reverseCount in reverseCounts.keys():
                if not(reverseCounts[reverseCount]==1 and examples[reverseCount]==None):
                    cells+=reverseCounts[reverseCount]
                    celltotal += Math.log10(reverseCount)* reverseCounts[reverseCount]
        
            if cells>0:
                cellavg = 10**(celltotal / cells)
        
                if trace:
                    print "avg occurrence", cellavg
            
        
            standouts={}
            for reverseCount in reverseCounts.keys():
                #print reverseCount
                if reverseCounts[reverseCount]==1 and examples[reverseCount]==None:
                    standouts["Nulls"]=reverseCount
                    if trace:
                        print "Null occurs", reverseCount, "times."
                if reverseCount == count:
                    columnType = "unused"
                elif reverseCount >= (0.9 * count):
                    columnQualifier += "sparselyUsed "
                else:
                    if trace:
                        print "There is/are",reverseCounts[reverseCount], "value(s) that occurs", reverseCount, "times. eg",examples[reverseCount]
                occurrence = {}
                occurrence["repeatCount"]=reverseCount
                occurrence["valueCount"]=reverseCounts[reverseCount]
                occurrence["example"]=examples[reverseCount]
                if reverseCount > 100 * cellavg:
                    if trace:
                        print "*** HIGH OCCURRENCE"
                    standouts["highFrequency-"+str(reverseCount)]=occurrence
                    if columnQualifier.find("unbalanced")<0:
                        columnQualifier += "unbalanced "
                elif reverseCount <  cellavg / 50:
                    if trace:
                        print "*** LOW OCCURRENCE"
                    standouts["lowFrequency-"+str(reverseCount)]=occurrence
                    if columnQualifier.find("unbalanced")<0:
                        columnQualifier += "unbalanced "
                  
            columnDict["standouts"]=standouts
              
            if columnType == "data":
                if distinctCount == 1:
                    columnType = "constant"
                elif distinctCount < 10:
                    columnType = "smallCategorisation"
                elif distinctCount < 40:
                    columnType = "categorisation"
                elif distinctCount > count/2:
                    columnType = "key"
                      
            #print "information content:",collapse(condense(sorted(prepare(reverseCounts))))
            columnDict["information"]=collapse(condense(sorted(prepare(reverseCounts))))[0][2]
                      
        if trace:
            print column[0],"looks like",columnQualifier+columnType
        columnDict["looksLike"]=columnType
        columnDict["qualifier"]=columnQualifier
        if trace:
            print 
        tableDict["tableColumns"][column[0]]=columnDict
    return tableDict


def prettyPrint(prefix, name, struc):
    print prefix+name+":",
    if type(struc)!=dict:
        print str(struc)
    else:
        print
        for key in struc.keys():
            prettyPrint(prefix+".   ", key, struc[key])
        
def xmlise(name, struc):
    xml="<"+name+">"
    if type(struc)!=dict:
        xml+= str(struc)
    else:
        for key in struc.keys():
            xml+=xmlise(key, struc[key])
    xml+="</"+name+">"
    return xml
            
            

#prettyPrint("", "test", {"a":10,"b":20})

#print xmlise("table",analyseTable("tsentityversion"))
print xmlise("table",analyseTable("identifiedentity"))
#analyseTable("property")
#analyseTable("datetime")

#print xmlise("table",analyseTable("qrtz_job_details"))
#analyseTable("qrtz_cron_triggers")

###