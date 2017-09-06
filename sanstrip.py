import re, os, csv
import datetime as dt
from bs4 import BeautifulSoup as bs
from peewee import *

db = SqliteDatabase('santran.db')

class Transaction(Model):
	ldate = DateField()
	note = CharField()
	value = FloatField()
	balance = FloatField()
	tdate = DateField()
	payee = CharField()
	transtype = CharField()

	class Meta:
		database = db

def db_create():
	db.connect()
	db.create_tables([Transaction])

def quickquery():
	for t in Transaction.select():
		print t.ldate, t.note

def readandstore(file_in):
	f = open(file_in,'read')
	lx = f.readlines()
	# clean file of \r\n
	nx = []
	for l in lx:
		new = re.sub('\\r\\n','',l)
		nx.append(new)
	lx = nx
	# handle transactions line by line
	for l in lx:
		new_trans = Transaction()
		# layout: ldate, note, val, bal, tdate    
		tx = re.split(';',l)
		new_trans.ldate = dt.datetime.strptime(tx[0],'%d/%M/%Y')
		new_trans.tdate = dt.datetime.strptime(tx[4],'%d/%M/%Y')
		new_trans.note = tx[1]
		new_trans.value = tx[2]
		new_trans.balance = tx[3]

		# calculated values
		new_trans.payee = GetPayee(tx[1]).strip()
		new_trans.transtype = GetType(tx[1]).strip()

		new_trans.save()

def san2csv(file_in,file_out):
	# now = dt.datetime.utcnow().strftiime('%Y-%m-%d-%H-%M-%S')
	# file_out = "trans-%s.csv" % (now)

	fr = open(file_in,'read')
	html = fr.read()
	soup = bs(html,'html.parser')

	tb = soup.table
	trx = soup.find_all('tr')

	mx = [] 
	for tr in trx:
		m = []
		ncount = 1
		for c in tr.children:
			if c.get_text():
				if ncount == 2 or ncount == 4:
					text = c.get_text().strip()
					text = re.sub("\\xa3","",text)
					text = re.sub(",","",text)
					try:
						val = float(text)
						m.append(val)
					except ValueError:
						m.append(text)
				elif ncount == 3:
					text = c.get_text().strip()
					text = re.sub("\\xa3","",text)
					text = re.sub(",","",text)
					try:
						val = float(text) * -1
						m.append(val)
					except ValueError:
						m.append(text)
				else:
					m.append(c.get_text().strip())
				ncount += 1
		mx.append(m)

	lx = [] #genuine trans
	rx = [] #rejected lines
	pd = re.compile('(\d{2}/\d{2}/\d{4})')
	for m in mx:
		if len(m) > 0:
			date_field = m[0]
			resd = pd.match(m[0])
		if resd:
			lx.append(m)
		else:
			rx.append(m)

	print "Total:%s - success:%s - failed:%s" % (len(mx),len(lx),len(rx))

	fx = []
	p1 = re.compile('(-?[\d|,]+\.\d+)$') #https://regex101.com/r/PfhQeZ/1
	p2 = re.compile('\sON\s(\d{2}-\d{2}-\d{4})')
	pd = re.compile('(\d{2}/\d{2}/\d{4})')
	date_check = False

	nlim = 10000
	ncnt = 0
	error_count = 0

	for l in lx:
		if nlim > ncnt:
			if len(l) > 0:
				date_field = l[0]
				resd = pd.match(l[0])
			if resd:
				f = []
				for c in l:
					try:
						res2 = p2.search(c)
						if res2:
							date = res2.groups()[0]
							date = re.sub('-','/',date)
							date_check = True
					except TypeError:
						error_count += 1
					if c <> 'GBP':
						f.append(c)
				if not date_check:
					date  = l[0]
					date_check = True
				f.append(date)
				fx.append(f)
			ncnt += 1

	fw = open(file_out,'append')
	csvwr = csv.writer(fw,delimiter=';',quoting=csv.QUOTE_NONE)
	# csvwr.writerow(['ledgerdate','description','val','bal','transdate'])

	for f in fx:
		csvwr.writerow(f)

	fw.close()

ttypes = ['BILL PAYMENT TO',
	'BILL PAYMENT FROM',
	'BILL PAYMENT VIA',
	'CARD PAYMENT',
	'DIRECT DEBIT PAYMENT',
	'APPLE PAY',
	'INTEREST PAID',
	'Cashback',
	'TRANSFER TO',
	'TRANSFER FROM',
	'CASH WITHDRAWAL',
	'BANK GIRO CREDIT',
	'PURCHASE FEE',
	'STANDING ORDER',
	'CREDIT FROM',
	'FASTER PAYMENTS RECEIPT',
	'ARRANGED OVERDRAFT USAGE FEE',
	'CASH PAID IN',
	'CASH WITHDRAWAL HANDLING CHARGE',
	'REGULAR TRANSFER',
	'PAYM'
	]

def GetType(inp):
	for t in ttypes:
		if t in inp:
			return t
	return 'none'

def GetTrueDate(inp, ndate):
	try:
		datestr = re.split(' ON ', inp)[1]
		if len(datestr) == 10:
			f = "%d-%m-%Y"
			tdate = datetime.strptime(datestr, f)
			return tdate
		else:
			return ndate
	except ValueError:
		return ndate
	
def GetPayee(inp):
	typ = GetType(inp)
	print typ
	try:
		if typ == 'APPLE PAY':
			payee = re.split('VIA', inp)
			payee = payee[0]
			nlen = len(payee) - 2
			payee = payee[:nlen]
			return payee
		elif typ == 'CARD PAYMENT':
			patt = re.compile('\sTO\s([a-z,A-Z,\s]*)\d')
			m = patt.search(inp)
			if m:
				payee = m.group(1)
			else:
				payee = 'unknown'
			return payee.strip()
		elif typ in ['DIRECT DEBIT PAYMENT', 'BILL PAYMENT TO', 'BILL PAYMENT VIA', 'PAYM','REGULAR TRANSFER']:
			payee = re.split(' REF', inp)
			payee = payee[0]
			payee = re.split(' TO ', payee)
			payee = payee[1]
			return payee
		elif typ == 'REGULAR TRANSFER':
			patt = re.compile('\sTO\s([a-z,A-Z,\s]*)\sMANDATE')
			m = patt.search(inp)
			if m:
				payee = m.group(1)
			else:
				payee = 'unknown-regtrans'
			return payee
		elif typ == 'TRANSFER TO':
			payee = re.split(' TO ', inp)
			payee = payee[1]
			return payee
		elif typ in ['BILL PAYMENT FROM','CREDIT FROM','TRANSFER FROM','FASTER PAYMENTS RECEIPT']:
			payee = re.split(' FROM ', inp)
			payee = payee[1]
			if ' ON ' in payee:
				payee = re.split(' ON ', payee)
				payee = payee[0]
			return payee
		elif typ in ['CASH WITHDRAWAL', 
						'INTEREST PAID', 
						'cashback', 
						'BANK GIRO CREDIT', 
						'PURCHASE FEE',
						'ARRANGED OVERDRAFT USAGE FEE',
						'CASH PAID IN',
						'CASH WITHDRAWAL HANDLING CHARGE']:
			return 'no payee'
		else:
			return 'unknown'
	except IndexError:
		return 'unknown-indexerror'