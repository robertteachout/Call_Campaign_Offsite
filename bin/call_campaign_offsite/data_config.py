from pathlib import Path
from glob import glob
import pandas as pd
import os
from zipfile import ZipFile
import zipfile
import shutil

### Input/output static tables ###
def tables(push_pull, table, name):
    table_path = Path("data/table_drop/")
    if push_pull == 'push':
        table.to_csv(table_path / name, sep=',',index=False)
    else:
        return pd.read_csv(table_path / name, sep=',',low_memory=False)

### push_pull zip file ###
def zipfiles(push_pull, df, filename):
    if push_pull == 'pull':
        Extract_path = Path("data/extract")
        file = (list(Extract_path.glob(filename))[0])
        with ZipFile(file, 'r') as zip:
                zip_name = ",".join(zip.namelist())
                Extract = pd.read_csv(zip.extract(zip_name), sep='|', on_bad_lines='skip', engine="python")
                os.remove(zip_name)
                return Extract
    else:
        with ZipFile(filename + '.zip', 'w', compression=zipfile.ZIP_DEFLATED) as zip:
                path = str(filename + '.csv')
                df.to_csv(path, sep=',',index=False)
                zip.write(path)
                zip.close()
                os.remove(path)
                pathfile = Path("data/load") / str(filename + '.zip')
                if os.path.exists(pathfile):
                    os.remove(pathfile)
                    shutil.move(str(filename + '.zip'), Path("data/load"))
                else:
                    shutil.move(str(filename + '.zip'), Path("data/load"))
                


