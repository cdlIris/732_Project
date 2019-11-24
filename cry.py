import cryptocompare
import requests
list=cryptocompare.get_coin_list(format=True)
#print(list)
import time
from kafka import KafkaProducer
import json
from kafka import KafkaConsumer
'''url='https://min-api.cryptocompare.com/data/v2/histominute?fsym=ETH&tsym=GBP&limit=1'#change the coin name in fsym='coinname' change how many minute you want to get in limit=''
r=requests.get(url)
data=r.json()
#print(data['Data'])
data1=data['Data']
timefrom=data1['TimeFrom']
timeto=data1['TimeTo']
data_all=data1['Data']
data_from=data_all[0]#data of every minute's start
data_to=data_all[1]#data of every minute's end'''
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def getbitcoin_minute(coinname):
    url = 'https://min-api.cryptocompare.com/data/v2/histominute?fsym='+str(coinname)+'&tsym=USD&limit=1'
    print(url)
    r = requests.get(url)
    data = r.json()
    print(data['Data'])
    data1 = data['Data']
    timefrom = data1['TimeFrom']
    timeto = data1['TimeTo']
    data_all = data1['Data']
    data_from = data_all[0]  # data of every minute's start
    data_to = data_all[1]  # data of every minute's end
    print(timefrom)
    print(timeto)
    print(data_from)
    print(data_to)
    #print(data_from['high'])

    msg = '%s %s %s %s %s %s %s %s %s %s %s %s %s %s' % (
    str(data_from['time']), str(data_from['high']), str(data_from['low']), str(data_from['open']),
    str(data_from['volumefrom']), str(data_from['volumeto']), str(data_from['close']), str(data_to['time']),
    str(data_to['high']), str(data_to['low']), str(data_to['open']), str(data_to['volumefrom']),
    str(data_to['volumeto']), str(data_to['close']))
    print(msg)
    producer.send('bitcoin_minute', msg.encode('utf-8'))

    #print(type(data_all[0]))


while True:
    coinname='BTC'
    getbitcoin_minute(coinname)
    time.sleep(60)