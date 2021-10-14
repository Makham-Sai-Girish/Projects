
import requests
import os
from bs4 import BeautifulSoup as bs
import pandas as pd

#need to automate this one (pipeline wil provide this vlaue)
pubmed_updated_url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/'


read_page = requests.get(pubmed_updated_url)
#print(read_page)

page_data = bs(read_page.content, 'html.parser')
#print(page_data)

under_pre = page_data.pre.contents
#print(under_pre)
#print(len(under_pre))
# pick the values with xml & html
under_pre = under_pre[6:]
#print(under_pre)

under_pre_gz = under_pre[0::4]

#print(under_pre_gz)
#under_pre_gz = under_pre.filter(html,gz)
#print(under_pre_gz)
names = [i.text.split(".") for i in under_pre_gz]
#print(names)
names = names[:-1]
print(names)

#mainurl+gznames

file_url = []
for i in under_pre_gz:
    c = pubmed_updated_url+i.text
    #call this as pubmed_url
    file_url.append(c)
file_url = file_url[:-1]
#print(file_url)
print(len(names))
#file_name = [pubmed_updated_url+names[i][0] for i in range(len(names))]
file_name = [names[i][0] for i in range(len(names))]
#print(file_name)
#print(len(file_name))

#print(len(names))
file_type = [names[i][1] for i in range(len(names))]
#print(file_type)
#print(len(file_type))

under_pre_details = under_pre[1::4]
#print(under_pre_details)

under_pre_details = [i.replace("\n", " ").strip().split(" ") for i in under_pre_details]
#print(under_pre_details)

date = [under_pre_details[i][0] for i in range(len(under_pre_details))]
#print(date)

time = [under_pre_details[i][1] for i in range(len(under_pre_details))]
#print(time)

file_size = [under_pre_details[i][-1] for i in range(len(under_pre_details))]
#print(file_size)

#create a DatFrame
df = pd.DataFrame({})
#data = [file_name,file_type,date,time,file_size,file_url]
#schema = ["File Name","File Type", "Date","Time","Size","File_URL" ]

#df = spark.createDataFrame(data=data, schema=schema)

#df  = pd.DataFrame(data=data, columns=schema)

df["File Name"] = file_name
df["File Type"] = file_type
df["Date"] = date
df["Time"] = time
df["Size"] = file_size
df["File_URL"] = file_url
#print(len(file_name))
#print(len(file_type))
#print(len(date))
#print(len(time))
#print(len(file_size))
#print(len(file_url))
print(df)

df.to_csv("pubmed_updatedfiles1.csv")