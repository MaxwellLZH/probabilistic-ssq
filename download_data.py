import re
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle as pkl
import os
from concurrent import futures


def fetch_shuangseqiu_result(url, use_default_encoding=False):
	page = requests.get(url)
	assert page.status_code == 200

	if use_default_encoding:
		page_content = page.content.decode(page.apparent_encoding)
	else:
		page_content = page.content.decode('GB18030')
		
	soup = BeautifulSoup(page_content, 'lxml')

	date = soup.find('span', attrs={'class': 'span_right'}).text
	year, month, day, *_ = re.findall('[0-9]+', date)
	date = datetime.date(int(year), int(month), int(day))

	red_balls = [int(e.text.strip()) for e in soup.find_all('li', class_='ball_red')]
	blue_ball = int(soup.find('li', class_='ball_blue').text.strip())

	try:
		sales, pool_size = re.findall('[0-9,-]+元', page_content)
		sales = float(sales[:-1].replace(',', '')) if '-' not in sales else None
		pool_size = float(pool_size[:-1].replace(',', '')) if '-' not in pool_size else None
	except:
		sales, pool_size = None, None

	price = soup.find_all('table', class_='kj_tablelist02')[1]
	# read_html doesn't work with bs4.Tag
	price: pd.DataFrame = pd.read_html(str(price))[0]

	# 新的页面里有额外信息需要剔除
	if price.shape[0] > 10:
		price = price.iloc[6: ].reset_index(drop=True)

	price_money, price_count = {}, {}
	for row_id, row in price.iterrows():
		# if row[0].endswith('等奖') and not row[0].startswith('幸运'):
		# 	price_count[row[0]] = 0 if '--' in row[1] else float(row[1])
		# 	price_money[row[0]] = 5000000 if '--' in row[2] else float(row[2])

		if 2 <= row_id <= 7:
			price_count[row_id-1] = 0 if '--' in row[1] else float(row[1])
			price_money[row_id-1] = 5000000 if '--' in row[2] else float(row[2])

	return {'date': date, 'red_balls': red_balls, 'blue_ball': blue_ball, 
			'sales': sales, 'pool_size': pool_size,
			'price_count': price_count, 'price_money': price_money}



def download_shuangseqiu(save_dir=None, use_default_encoding=False):
	try:
		from tqdm import tqdm
	except:
		def tqdm(obj):
			return obj

	save_dir = save_dir or './data/'
	save_path = os.path.join(save_dir, 'shuangseqiu.pkl')
	try:
		os.mkdir(save_dir)
	except:
		pass

	url = 'http://kaijiang.500.com/shtml/ssq/19002.shtml'
	page = requests.get(url)
	assert page.status_code == 200
	page_content = page.content.decode('gbk')
	soup = BeautifulSoup(page_content, 'lxml')
	print(soup)

	all_urls = [i.get('href') for i in soup.find_all('a') if i.get('href', '').startswith('https://kaijiang.500.com/shtml/ssq/')]
	all_urls = sorted(all_urls)[-100:]
	print('Total number of urls:', len(all_urls))

	res = []
	for url in tqdm(all_urls):
		try:
			res.append(fetch_shuangseqiu_result(url, use_default_encoding=use_default_encoding))
		except Exception as e:
			try:
				res.append(fetch_shuangseqiu_result(url, use_default_encoding=True))
			except Exception as e:
				print(e)
				print('Failed: {}'.format(url))

	with open(save_path, 'wb') as f:
		pkl.dump(res, f)

	print('Data download complete. Number of records: {}'.format(len(res)))



if __name__ == '__main__':
	download_shuangseqiu()
