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
        if conn.lower()=='epi':
            conn = 'mysql+pymysql://tableau:12tableau34@sqlsvr.solarjunction.local:3306/epi'
        elif conn.lower()=='db00' or conn.lower()=='pd' \
        or conn.lower() == 'eel' or conn.lower() =='el' \
        or conn.lower()=='vl' or conn.lower()=='vcsel':
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
        "WHERE " + nameCol + "='" + str(uniqueName) + "';"
        return self.__getID_bySQL(sql)
    
    def getIDlist_byValue(self,table,idCol,colName,value,preID=0):
        sql = "SELECT t." + idCol + " As id\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + colName + " = '" + value + "'\r\n" + \
        "AND t." + "idCol > " + str(preID) + ";"
        return self.__getIDlist_bySQL(sql)
    
    def getIDlist_byValuelist(self,table,idCol,colName,valueList):
        sql = "SELECT t." + idCol + " As id\r\d" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + colName + " in ("
        condition = self.__getList_SQL(valueList)
        sql = sql + condition + ");"
        return self.__getIDlist_bySQL(sql)
    
    def getID_byID(self,table,idCol,ID):
        sql = "SELECT t." + idCol + " AS id\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + idCol + "=" + str(ID) + ";"
        return self.__getID_bySQL(sql)
    
    def getDF_all(self,table):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + ";"
        return pd.read_sql_query(sql,self.eg)
    
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
        condition = self.__getList_SQL(idList)
        sql = sql + condition + ");"
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_byUniqueName(self,table,nameCol,uniqueName):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + "\r\n" + \
        "WHERE " + nameCol + "='" + uniqueName + "';"
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_bySingleValue(self,table,Col,Value,idCol):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + "\r\n" + \
        "WHERE " + Col + "='" + str(Value) + "';"
        return pd.read_sql_query(sql,self.eg)
    
    def getDF_byValueList(self,table,Col,ValueList):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + Col + " in ("
        condition = self.__getList_SQL(ValueList)
        sql = sql + condition + ");"
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
    
    def getWaferNames_byWaferIDList(self,wafer_id_list):
        if self.schema == 'epi':
            df = self.getDF_byIDList('epi_wafer','wafer_id',wafer_id_list)
        return df['wafer_name']
    
    def getWaferName_byWaferID(self,wafer_id):
        if self.schema == 'epi':
            df = self.getDF_byID('epi_wafer','wafer_id',wafer_id)
        return df['wafer_name'].iloc[0]
    
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
    
    def sync_byDF_byuniqueName(self, table, idCol, nameCol, df):
        # if df[nameCol] in db, return the id
        # if df[nameCol] not in db, insert and return the id
        for index, row in df.iterrows():
            #ds is the current row but still in dataframe format
            ds = row.to_frame().transpose() #change data series to dataframe
            if idCol in ds.columns: 
                ds_noID = ds.drop(columns = [idCol])
            else:
                ds_noID = ds
            uName = ds[nameCol].iloc[0]
            ID = self.getID_byUniqueName(table,idCol,nameCol,uName)
            if ID != 0:
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
    
    def sync_byDF_grouptime(self,table,idCol,timeCol,df):
        dt_group = df[timeCol][0]
        if self.getID_byUniqueName(table,idCol,timeCol,dt_group)==0:
            df.to_sql(name=table,con=self.eg,schema=self.schema,if_exists='append',index=False,method='multi')
            isExist = False
        else:
            isExist = True
        return isExist
    
    def sync_byDF_SingleValueGroup(self,table,idCol,Col,df):
        # the Col is the column to determine add or not
        # df share the same value for Col
        value = df[Col].iloc[0]
        if self.getID_byUniqueName(table,idCol,Col,value)==0:
            
            if idCol in df.columns: 
                df_noID = df.drop(columns = [idCol])
            else:
                df_noID = df
            df_noID.to_sql(name=table,con=self.eg,schema=self.schema,if_exists='append',index=False,method='multi')
            isAdded = True
        else:
            isAdded = False
        df_return = self.getDF_bySingleValue(table,Col,value,idCol)
        return df_return, isAdded
    
    def sync_byDF_MultiValueGroup(self,table,idCol,Col,ValueList,df):
        # the Col is the column to determine add or not
        # only if ValueList is not in DB, add to DB
        df_exist = self.getDF_byValueList(table,Col,ValueList)
        # remove existed rows
        if df_exist.empty == False:
            df_final = df[~df[Col].isin(df_exist[Col])]
        else:
            df_final = df
            
        if df_final.empty == False:
            if idCol in df_final.columns:
                df_noID = df_final.drop(columns = [idCol])
            else:
                df_noID = df_final
            df_noID.to_sql(name=table,con=self.eg,schema=self.schema,if_exists='append',index=False,method='multi')
            is_Added = True
        else:
            is_Added = False
        df_return  = self.getDF_byValueList(table,Col,ValueList)
        return df_return, is_Added
    
    def del_byID(self,table,idCol,idValue):
        conn = self.eg.connect()
        trans = conn.begin()
        sql = "DELETE FROM " + self.schema + "." + table + " t \r\n" \
            + "WHERE t." + idCol + " = " + str(idValue) + ";"
        conn.execute(sql)
        trans.commit()
        conn.close()
        return
    
    def del_byIDList(self,table,idCol,idList):
        conn = self.eg.connect()
        trans = conn.begin()
        condition = self.__getList_SQL(idList)
        sql = "DELETE FROM " + self.schema + "." + table + " t \r\n" \
            + "WHERE t." + idCol + " in (" + condition + ");"
        conn.execute(sql)
        trans.commit()
        conn.close()
        return
    
    def truncate_table(self,table):
        print ('Truncate table ' + table + '! Are you sure?')
        ui = input()
        if ui=='y':
            sql = "TRUNCATE TABLE " + table + ";"
            self.runSQL(sql)
        return
    
    def getID_bySQL_public(self,idCol,sql):
        data = pd.read_sql_query(sql,self.eg)
        if data.empty == True:
            ID=0
        else:
            ID=data[idCol][0]
        return ID
    
    def runSQL(self,sql):
        conn = self.eg.connect()
        trans = conn.begin()
        conn.execute(sql)
        trans.commit()
        conn.close()
    
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
    
    def __getIDlist_bySQL(self, sql):
        data = pd.read_sql_query(sql,self.eg)
        return data['id'].tolist()
    
    def __getList_SQL(self,valueList):
        lst = ""
        for i in valueList:
            lst = lst + "'" + str(i) + "',"
        lst = lst[:-1]
        return lst
    
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


