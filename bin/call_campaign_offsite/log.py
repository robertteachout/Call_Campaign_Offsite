import logging

logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

def df_len(varname, df):
    logging.info(f'{varname}\t- len:{len(df)}')


if __file__ == '__main__':
    df = [1,2,3,4]
    df_len(df)