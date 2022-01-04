import logging

logging.basicConfig(filename='app.log',
                    filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

def df_len(name, df):
    try:
        ln = len(df)
    except:
        ln = 0
    logging.info(f'len: {ln} \t\t {name}')
    # logging.info(df.dtypes)

if __file__ == '__main__':
    df = [1,2,3,4]
    df_len(df)