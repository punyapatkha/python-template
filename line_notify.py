
import requests

url = 'https://notify-api.line.me/api/notify'
token = 'change to your token'
headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}

msg = 'test message'
r = requests.post(url, headers=headers , data = {'message':msg})
print r.text
