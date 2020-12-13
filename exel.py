import numpy as np
import pandas as pd
import sqlalchemy
from os import listdir
from os.path import isfile, join

CtrlTbl = 'PY_Control_XLS_Import'

server='vm-docker.ad.nebie.de'
user='python'
password='python123!'
database='playground'
port=1433

def sqlcol(dfparam):    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict

engine = sqlalchemy.create_engine('mssql+pymssql://' + user + ':'+password+'@'+server+'/'+database+'')
connection = engine.connect()

meta = sqlalchemy.MetaData()

tbl_ctrl = sqlalchemy.Table(CtrlTbl, meta, autoload=True, autoload_with=engine)
query_ctrl = sqlalchemy.select([tbl_ctrl])

ResultProxy = connection.execute(query_ctrl)
ResultSet = ResultProxy.fetchall()

for res in ResultSet:
    data_dir = res[1]
    data_file = res[2]
    DestTbl = res[3]
    RowsToSkip = res[5]
    data_sheets = res[4]
    TruncateOnLoad = res[6]
    tbl_insert = None
    excl_sheets = []
    incl_sheets = []
    if data_sheets[0:1] == "!":
        excl_sheets = data_sheets[1:len(data_sheets)].split(',')
    if '-' in data_sheets:
        from_to     =   data_sheets.split('-')
        sheet_from  =   int(from_to[0])
        sheet_to    =   int(from_to[1])
    else:
        incl_sheets = data_sheets.split(',')
  
    if (engine.dialect.has_table(engine.connect(), DestTbl)):
        tbl_insert = sqlalchemy.Table(DestTbl, meta, autoload=True, autoload_with=engine)
        if TruncateOnLoad == True:
            connection.execution_options(autocommit=True).execute("TRUNCATE TABLE [" + DestTbl + "];")

    for filename in listdir(data_dir):
        if (filename == data_file or data_file is None) and filename[-5:] == ".xlsx" and filename[0:1] != '~':
            if (engine.dialect.has_table(engine.connect(), DestTbl) and tbl_insert is None):
                tbl_insert = sqlalchemy.Table(DestTbl, meta, autoload=True, autoload_with=engine)
            fullpath = data_dir + "\\" + filename

            xl = pd.ExcelFile(fullpath)
            sheet_i = 1
            for sheet in xl.sheet_names:
                if (len(excl_sheets) > 0 and sheet not in excl_sheets) or (len(incl_sheets) > 0 and sheet in incl_sheets) or (sheet_i >= sheet_from and sheet_i <= sheet_to):
                    excel_df = pd.read_excel(fullpath, skiprows=RowsToSkip, sheet_name=sheet)
                    excel_df['Meta_Filename'] = filename
                    excel_df['Meta_RowNumber'] = np.arange(excel_df.shape[0])
                    excel_df['Meta_Sheetname'] = sheet
                    outputdict = sqlcol(excel_df) 
                    meta = sqlalchemy.MetaData()
                    if tbl_insert is not None:
                        i = 0
                        for col in tbl_insert.columns:
                            tbl_col_name = col.name
                            col.type.collation = None
                            tbl_col_type = col.type
                            
                            if i < len(excel_df.columns):
                                df_col_name = excel_df.columns[i]
                                df_col_type = outputdict[df_col_name]
                        #        if str(df_col_type) != str(tbl_col_type):
                        #            print("No match")
                                if tbl_col_name != df_col_name:
                                    excel_df.rename(columns={ excel_df.columns[i]: tbl_col_name }, inplace = True)
                            i = i + 1

                    while True:
                        try:
                            excel_df.to_sql(DestTbl, engine, if_exists='append', index=False , dtype = outputdict)
                        except BaseException as e:
                            b = e.args
                            col = b[0].split('(pymssql.ProgrammingError) (207, b"Invalid column name \'')[1].split('\'')[0]
                            print(str(excel_df[col].dtype))
                            if "int" in str(excel_df[col].dtype):
                                res = engine.execute("ALTER TABLE ["+DestTbl+"] ADD "+col+" INT")
                            if "float" in str(excel_df[col].dtype):
                                res = engine.execute("ALTER TABLE ["+DestTbl+"] ADD "+col+" FLOAT")
                            if "datetime" in str(excel_df[col].dtype):
                                res = engine.execute("ALTER TABLE ["+DestTbl+"] ADD "+col+" DATETIME")
                            if "object" in str(excel_df[col].dtype):
                                res = engine.execute("ALTER TABLE ["+DestTbl+"] ADD "+col+" NVARCHAR("+str(4000)+")")
                            print("Spalte " + col + " angelegt.")
                        else: break
                    print("Imported File: " + filename + "; Imported Sheet: " + sheet)
                sheet_i = sheet_i + 1