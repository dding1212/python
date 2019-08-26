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
        self.schema = schema  #epi or test
        if conn=='epi' or conn=='Epi' or conn=='EPI':
            conn = 'mysql+pymysql://tableau:12tableau34@sqlsvr.solarjunction.local:3306/epi'
        elif conn=='DB00' or conn=='db00' or conn=='pd' or conn=='PD':
            conn = 'mysql+pymysql://tableau:12tableau34@192.168.59.30:3306/test'
        
        self.eg = create_engine(conn, echo=False)
    
    def close(self):
        # dispose the engine
        self.eg.dispose()

    def getID_byUniqueName(self,table,idCol,nameCol,uniqueName):
        sql = "SELECT t." + idCol + " AS id\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + nameCol + "='" + uniqueName + "';"
        return self.__getID_bySQL(sql)
    
    def getDF_byID(self,table,idCol,idValue):
        sql = "SELECT *\r\n" + \
        "FROM " + self.schema + "." + table + " t\r\n" + \
        "WHERE " + idCol + "=" + str(idValue) + ";"
        return pd.read_sql_query(sql,self.eg)
    
    def getID_byDF(self, table, idCol, df):
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
    
    def getLastID(self, table, idCol):
        sql = "SELECT t." + idCol + " as id\r\n" \
             + "FROM " + self.schema+ "." + table + " t\r\n" \
             + "ORDER BY t." + idCol + " DESC LIMIT 0, 1"
        return self.__getID_bySQL(sql)

    def sync_byDF(self, table, idCol, df):
        # if df in db, return the id
        # if df not in db, insert and return the id
        for index, row in df.iterrows():
            #ds is the current row but still in dataframe format
            ds = row.to_frame().transpose() #change data series to dataframe
            ds_noID = ds.drop(columns = [idCol])
            ID = self.get_id_byDF(table,idCol,ds_noID)
            #print(ID)
            isna = row.isna()
            if ID != 0 and isna[idCol] == True:
                df.at[index,idCol] = ID #set ID
            elif ID != 0 and row[idCol] != ID:
                df.at[index,idCol] = ID # correct ID, normally it shouldn't happen
            elif ID == 0:
                # add to DB and return ID
                ID = self.__insert_single(table,idCol,ds_noID)
                df.at[index,idCol] = ID
        # df to be returned has all the updated IDs
        return df
    
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

    db = dbConn('PD','test',connPD)
    df = db.getDF_byID('pd_rawtest','rawtest_id',2)
    ID = db.getID_byDF('pd_rawtest','rawtest_id',df)
    LastID = db.getLastID('pd_rawtest','rawtest_id')
    maskID = db.getID_byUniqueName('pd_mask','mask_id','mask_name','PH006')
    db.close()


