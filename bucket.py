#! /usr/bin/env python3

import boto3, datetime, sys, getopt, re
from operator import itemgetter
from table_logger import TableLogger

class buck: 
	TARIFAWS = 0.023 # 0,023 USD por GB (Primeiros 50 TB por mês)


	counter = 0 

	def __init__(self, s3bucket):
		buck.counter = buck.counter + 1
		self.name = s3bucket.name 
		self.creationdate = s3bucket.creation_date
		self.size = self.metricCloudwatch(s3bucket,"BucketSizeBytes", "StandardStorage")
		self.nbreObj = self.metricCloudwatch(s3bucket,"NumberOfObjects", "AllStorageTypes")
		
		try: 
			boto3.client('s3').get_bucket_encryption(Bucket=s3bucket.name)
			self.number = True
		except:
			self.number = False
		
		self.region = (boto3.client('s3').get_bucket_location(Bucket=s3bucket.name))['LocationConstraint'] 
		
		self.cout = round(self.size / 1024**3 * self.TARIFAWS,2) 
				
		try: 
			boto3.client('s3').get_bucket_replication(Bucket=s3bucket.name)
			self.replica = True
		except:
			self.replica = False

		def collObjInfo(self):
			s3obj = (boto3.client('s3')).list_objects_v2(Bucket=self.name)
			self.lastUpdate = None
			self.typeStorage = None
			if s3obj['KeyCount'] != 0:
				self.lastUpdate = s3obj['Contents'][0]['LastModified']
				self.typeStorage = s3obj['Contents'][0]['StorageClass']
			 
		collObjInfo(self)
		self.public = False 


	def __str__(self):
		return str(self.__class__) + ": " + str(self.__dict__)

	def __getitem__(self, key):
		if key == 'region':
			return self.region
		if key == 'typeStorage':
			return self.typeStorage

	def getSize(self, human=False): 
		if human:
			return humanReadable(self.size)
		else:
			return self.size

	def metricCloudwatch(self, bucket, nameMetric, storage): 
		cloudwatch = boto3.client('cloudwatch')
		now = datetime.datetime.now()
		try:
			cloudwatch_size = cloudwatch.get_metric_statistics(
				Namespace='AWS/S3',
				MetricName=nameMetric,
				Dimensions=[
					{'Name': 'BucketName', 'Value': bucket.name},
					{'Name': 'StorageType', 'Value': storage}
				],
				Statistics=['Maximum'],
				Period=86400,
				StartTime=(now - datetime.timedelta(days=1)).isoformat(),
				EndTime=now.isoformat()
			)
			if cloudwatch_size["Datapoints"]:
				return cloudwatch_size["Datapoints"][0]['Maximum']
			else:
				return 0
		except:
			return 0
		
def humanReadable(num, suffix='B'): 
    for unit in ['','K','M','G','T','P']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def help(): 
	print("Uso : bucket.py [OPTIONS]")
	print("Exibe informações sobre buckets AWS S3, por padrão")
	print("Argumentos : \n\
	--help \t\t\t ajuda\n\
	--crypted-only \t\t mostra apenas buckets criptografados\n\
	-c, --csv \t\t mostra o resultado em CSV\n\
	-s, --sorted \t\t agrupar resultados por região e grupo de armazenamento\n\
 	-h, --human-readable \t exibem tamanhos de 1024\n\
	-f, --filter=filter \t filtra a lista de buckets com base na expressão regular FILTER") 
			

def main():
	
	csv=False
	human = False
	group = False
	filterCrpt = False
	filter = None
	
	try:
		opts, args = getopt.getopt(sys.argv[1:], "shcf:", ["sorted", "help", "csv", "human-readable", "crypted-only", "filter:"])
	except:
		print("Comando incorreto, aqui está a ajuda: ")
		help()
		sys.exit(2)

	for opts, args in opts:
		if opts == "--help":
			help()
			sys.exit()
		elif opts == "--crypted-only":
			filterCrpt = True
		elif opts in ("-c", "--csv"):
			csv = True
		elif opts in ("-s", "--sorted"):
			group = True
		elif opts in ("-h", "--human-readable"):
			human = True
		elif opts in ("-f", "--filter"):
			if len(args):
				filter = args
			else:
				help()
				sys.exit(2)
		
	s3 = boto3.resource('s3')
	bucks = [] 

	listeS3Bucks = s3.buckets.all()
	for bucket in listeS3Bucks:
		try:
			if filter:
				re.match(filter,"Test chain")
		except:
			print("Regular expression error")
			sys.exit(2)

		if (filter and re.match(filter,bucket.name)) or not filter:
			try:
				bucks.append(buck(bucket)) 
			except:
				print("Erro ao conectar ao AWS, verifique suas configurações")
				print("Para obter mais informações: https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html")
				sys.exit(2)

	if group: 
		bucks = sorted(bucks, key=itemgetter('region'))
		bucks = sorted(bucks, key=itemgetter('typeStorage'))
		
	tbl = TableLogger(columns='name,creation date,last update,size,number of objects,number,storage,public,region,cost,replica',
		csv=csv, border=False) 
		
	for cBuck in bucks:
		if (filterCrpt and cBuck.number) or not filterCrpt:
			tbl(cBuck.name, cBuck.creationdate, cBuck.lastUpdate, cBuck.getSize(human), str(cBuck.nbreObj), 
			cBuck.number,cBuck.typeStorage, cBuck.public, cBuck.region, "$"+str(cBuck.cout),cBuck.replica)
		
if __name__ == "__main__":
	main()
