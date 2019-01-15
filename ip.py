import requests
import telnetlib
from bs4 import BeautifulSoup
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.\
        0.3578.98 Safari/537.36"
    }


def get_html(page_number):
    base_url = "https://www.xicidaili.com/wn/"
    for i in range(1, page_number+1):
        url = base_url + str(i)
        response = requests.get(url, headers=headers).text
        soup = BeautifulSoup(response, 'lxml')
        types = [i.get_text() for i in soup.select("tr > td:nth-of-type(6)")]
        ip_address = [i.get_text() for i in soup.select('tr > td:nth-of-type(2)')]
        ip_ports = [i.get_text() for i in soup.select("tr > td:nth-of-type(3)")]
        for type, ip_addres, ip_port in zip(types, ip_address, ip_ports):
            data = {
                'type': type,
                'ip_address': ip_addres,
                'ip_port': ip_port
            }
            # if data['ip_port'] != '9999':
            #     verify_ip(data['ip_address'], data['ip_port'])
            # else:
            #     pass
            ip = data['type'] + '://' + data['ip_address'] + ':' + data['ip_port']
            verify_ip(ip)


def verify_ip(ip):
    try:
        response = requests.get('https://gz.lianjia.com/ershoufang/', headers=headers, proxies={'https': ip}, timeout=1)
        if response.status_code == 200:
            with open('ip.txt', 'a') as f:
                print("正在存储IP：" + ip)
                f.write("\'" + ip + "\'," + '\n')
        else:
            pass
        # telnetlib.Telnet(ip_agent, ip_port, timeout=1)  # 利用Telnet函数实现ip检测
    except:
        pass


if __name__ == '__main__':
    page_number = int(input("输入你想要抓取的ip页数：", ))
    get_html(page_number)
