# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 09:43:38 2019
This dbConn Class is for database connection and simple operation
@author: dding
"""
import pandas as pd
from sqlalchemy import create_engine

class dbConn():

    def __init__(self,name,schema,conn):
        self.name = name      #can be any name
        self.schema = schema  #epi,test, or laser
        if conn=='epi' or conn=='Epi' or conn=='EPI':
            conn = 'mysql+pymysql://tableau:12tableau34@sqlsvr.solarjunction.local:3306/epi'
        elif conn=='DB00' or conn=='db00' or conn=='pd' or conn=='PD' \
        or conn=='EEL' or conn=='eel' or conn =='el' \
        or conn=='VL' or conn=='vl' or conn=='VCSEL':
             conn = 'mysql+pymysql://tableau:12tableau34@192.168.59.30:3306/'+schema
        
        self.eg = create_engine(conn, echo=False)
    
    def close(self):
        # dispose the engine
        self.eg.dispose()
    
    def initDF_byTable(self,table):
        sql = "show columns from " + self.schema +"." + table
        data = pd.read_sql_query(sql,self.eg)
        df = pd.DataFrame(columns=data["Field"])
        return df
    
    def getID_byUniqueName(self,table,idCol,nameCol,uniqueName):
        sql = "SELECT t." + idCol + " AS id\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + nameCol + "='" + uniqueName + "';"
        return self.__getID_bySQL(sql)
    
    def getID_byID(self,table,idCol,ID):
        sql = "SELECT t." + idCol + " AS id\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + idCol + "=" + str(ID) + ";"
        return self.__getID_bySQL(sql)

    def getDF_byID(self,table,idCol,idValue,idSort='',limit=0):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + idCol + "=" + str(idValue) 
        if limit==0 or idSort=='':
            sql = sql + ";"
        else:
            sql = sql + "\r\n" + \
            "ORDER BY t." + idSort + " DESC LIMIT " + str(limit) + ";"
        #print(sql)
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_byIDList(self,table,idcol,idList):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + idcol + " in (" 
        condition = ""
        for i in idList:
            condition = condition + str(i) + ","
        condition = condition[:-1]
        sql = sql + condition + ");"
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_byUniqueName(self,table,nameCol,uniqueName):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + "\r\n" + \
        "WHERE " + nameCol + "='" + uniqueName + "';"
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_bySQL(self,sql):
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_by_ID_joinedTables(self,table1,table2,idJoinCol,idCol,idValue):
        #idCol is for table1
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table1 + " t1\r\n" + \
        "INNER JOIN " + self.schema + "." + table2 + " t2 on " + \
        "t1." + idJoinCol + "=" "t2." + idJoinCol + "\r\n" + \
        "WHERE " +idCol + "=" + str(idValue) + ";"
        return pd.read_sql_query(sql,self.eg)
    
    def getID_byDF(self, table, idCol, df, skipCols=[]):
        if skipCols: # act only if skipCols is not empty
            if type(skipCols)=='str':
                df = df.drop(columns=[skipCols])
            else:
                df = df.drop(columns=skipCols)
        sql = "SELECT t." + idCol + " AS id\r\n" + \
        "FROM " + self.schema + "." + table +" t\r\n" + \
        "WHERE"
        for column in df:
            if column != idCol:
                if df[column].dtypes == 'float64':
                    strValue = " LIKE " + format(df.iloc[0][column], "g")
                else:
                    strValue = "='" + str(df.iloc[0][column]) +"'"
                sql = sql + " t." + column + strValue + " AND"
        sql = sql[:-3] + ";"
        #print(sql)
        return self.__getID_bySQL(sql)
    
    def getWaferList_byLotName(self,lotName,stage=310):
        if self.schema == 'epi':
            df = self.getDF_byUniqueName('wts_default_view','sjc_lot_number', lotName)
            df_filtered = df[df['manufacturing_stage_id']==stage]
            lst_wid = df_filtered['wafer_id'].tolist()
            lst_wname = df_filtered['wafer_name'].tolist()
            return lst_wid, lst_wname, df_filtered
    
    def getLastID(self, table, idCol):
        sql = "SELECT t." + idCol + " as id\r\n" \
             + "FROM " + self.schema+ "." + table + " t\r\n" \
             + "ORDER BY t." + idCol + " DESC LIMIT 0, 1"
        return self.__getID_bySQL(sql)

    def sync_byDF(self, table, idCol, df, skipCols=""):
        # if df in db, return the id
        # if df not in db, insert and return the id
        for index, row in df.iterrows():
            #ds is the current row but still in dataframe format
            ds = row.to_frame().transpose() #change data series to dataframe
            if idCol in ds.columns: 
                ds_noID = ds.drop(columns = [idCol])
            else:
                ds_noID = ds
            ID = self.getID_byDF(table,idCol,ds_noID,skipCols)
            #print(ID)
            #isna = row.isna()
            if ID != 0 :
                df.at[index,idCol] = ID #set ID
            elif ID == 0:
                # add to DB and return ID
                ID = self.__insert_single(table,idCol,ds_noID)
                df.at[index,idCol] = ID
        # df to be returned has all the updated IDs
        return df
    
    def sync_byDF_group(self, table, idCol, df):
        # the idCol is the unique group id, not the row id
        id_group = df[idCol][0]
        if self.getID_byID(table,idCol,id_group)==0:
            df.to_sql(name=table,con=self.eg,schema=self.schema,if_exists='append',index=False,method='multi')
            isExist = False
        else:
            isExist = True
        return isExist
    
    def __insert_single(self, table, idCol, df):
        # df is a single line datafram without id
        conn = self.eg.connect()
        trans = conn.begin()
        cols, values = self.__get_insert_values(df)
        sql = "INSERT INTO " + self.schema + "." + table + cols + "\r\n" \
            + "VALUES " + values
        conn.execute(sql)
        trans.commit()
        conn.close()
        return self.getLastID(table, idCol)
    
    def __get_insert_values(self, df):
        isna = df.isna()
        cols = "("
        values = "("
        for column in df:
            if isna.iloc[0][column] == False:
                cols  = cols + "`" + column + "`, "
                values = values + "'" + str(df.iloc[0][column]) + "', "
        cols = cols[:-2] + ")"
        values = values[:-2] + ")"
        return cols, values
        
    def __getID_bySQL(self, sql):
        data = pd.read_sql_query(sql,self.eg)
        if data.empty == True:
            ID=0
        else:
            ID=data['id'][0]
        return ID
    
if __name__ == "__main__":
    connEpi = 'mysql+pymysql://tableau:12tableau34@sqlsvr.solarjunction.local:3306/epi'
    connPD = 'mysql+pymysql://tableau:12tableau34@192.168.59.30:3306/test'
    db = dbConn('epi','epi','epi')
    wid,wname,df = db.getWaferList_byLotName('SJC_ENG_421_4-GaAs')
    
    #pl = db.getDF_by_ID_joinedTables('epi_pl2_meas','epi_pl2_meas_values','pl2_meas_id','wafer_id',38647)
    #table,idCol,idValue,limit=0
    #pl1 = db.getDF_byID('epi_pl2_meas','wafer_id',38647,limit=1)
#    db = dbConn('PD','test',connPD)
#    df = db.getDF_byID('pd_rawtest','rawtest_id',2)
#    ID = db.getID_byDF('pd_rawtest','rawtest_id',df,['laser_mw','i_meas_a'])
#    print(ID)
#    LastID = db.getLastID('pd_rawtest','rawtest_id')
#    maskID = db.getID_byUniqueName('pd_mask','mask_id','mask_name','PH006')
#    try_ID = db.getID_byID('pd_mask','mask_id',100)
    db.close()


