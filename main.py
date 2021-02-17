#!/usr/bin/env python3
# coding: utf-8

# In[1]:


import requests, re, json, os
from bs4 import BeautifulSoup as BS
from urllib.parse import urlparse
from datetime import datetime
import dateutil.parser as date_parser
from typing import List, Dict, Callable


# In[22]:


class Parser:
    def __init__(self, filename, ignore_actuality = False) -> None:
        '''
        Инициализация
        '''
        self.filename: str = filename
        self.ignore_actuality = ignore_actuality


    def load_urls(self) -> None:
        '''
        Загружает список адресов рсс рассылок из файла
        '''
        with open(self.filename, 'r', encoding = 'utf-8') as f:
            self.urls: List[str] = list(set(f.read().split()))


    def get_urls(self) -> List[str]:
        '''
        Возвращает список адресов рсс рассылок из файла
        '''
        return self.urls


    def find_actual_urls(self) -> None:
        '''
        Находит актуальные адреса (те, которых нет в файле уже использованых ранее адресов)
        '''
        if self.ignore_actuality:
            self.actual_urls: List[str] = self.urls
            return

        if not os.path.exists('Already_used_URLs.csv'):
            with open('Already_used_URLs.csv', 'w', encoding = 'utf-8') as f:
                for url in self.urls:
                    f.write(f'{url}\n')
                used: List[str] = []
        else:
            with open('Already_used_URLs.csv', 'r+', encoding = 'utf-8') as f:
                used: List[str] = list(set(f.read().split()))
        self.actual_urls: List[str] = [url for url in self.urls if url not in used]


    def get_actual_urls(self) -> List[str]:
        '''
        Возвращает актуальные адреса
        '''
        return self.actual_urls

    
    def download_data(self) -> None:
        '''
        Скачивает данные и создает словарь вида {'source' : [массив источников], 'data' : [массив данных]}
        '''
        assert len(self.actual_urls) != 0, 'Пустые УРЛы!'
        self.data: Dict[str, List[str]] =  {'source' : [urlparse(url).netloc for url in self.actual_urls], 
                                            'data' : [requests.get(url).text for url in self.actual_urls]}

            
    def get_data(self) -> Dict[str, List[str]]:
        '''
        Возвращает словарь данных
        '''
        return self.data


    def save_to_xml(self) -> None:
        '''
        Сохраняет данные в папку XML (каждый источник в отдельный файл)
        '''
        assert len(self.data) != 0, 'Пустые данные!'
        if not os.path.exists('XML'):
            os.mkdir('XML')
        for current_source, current_data in zip(self.data['source'], self.data['data']):
            count: int = self.data['source'].count(current_source)
            if count > 1:
                for i in range(count):
                    with open(
                        f'XML/{current_source.replace("www.", "").split(".")[0]}-{i}.xml',
                        'w', encoding = 'utf-8'
                    ) as f:
                        f.write(current_data)
            else:
                with open(
                    f'XML/{current_source.replace("www.", "").split(".")[0]}.xml',
                    'w', encoding = 'utf-8'
                ) as f:
                        f.write(current_data)

                        
    def save_to_one_xml(self) -> None:
        with open('common_xml.xml', 'w', encoding = 'utf-8') as f:
            for current_data in self.data['data']:
                f.write(f'{current_data}\n\n')
        

    def create_soup(self) -> None:
        '''
        Создает список супов
        '''
        self.soup: List[Callable] = [BS(data_i.replace('windows-1251', 'utf-8'), 'xml')
                                     for data_i in self.data['data']]


    def get_soup(self) -> List[Callable]:
        '''
        Возвращает список супов
        '''
        return self.soup


    def create_dict_with_fields(self, fields_json : List[str], tags : List[str]):
        '''
        Принимает на вход список полей, которые будут отображены в джсоне, и список тегов,
        которые должны соответствовать полям. Создает словарь вида:
        {'0' : {'title' : title, 'textBody' : textBody, ...}, ...}
        '''
        result: Dict[str, Dict[str, str]] = {}
        counter: int = 0
        for i in range(len(self.soup)):
            for j in range(1, len(self.soup[i].find_all('item'))):
                result[str(counter)] = {}
                for field, tag in zip(fields_json, tags):
                    if field == 'source':
                        try:
                            index: int = 0
                            expression: str = urlparse(self.soup[i].find_all('link')[index].string).netloc
                            while not expression:
                                index += 1
                                expression: str = urlparse(self.soup[i].find_all('link')[index].string).netloc
                            result[str(counter)][field]: str = re.sub(r'(\<(/?[^>]+)>)', '', expression)
                        except:
                            print(i, self.soup[i].find_all('link')[0].string)
                    elif tag == 'pubDate':
                        try:
                            result[str(counter)][field]: str = datetime.strftime(
                                date_parser.parse(
                                    self.soup[i].find_all(tag)[j].string
                                ), '%Y-%m-%d %H:%M'
                            )
                        except:
                            result[str(counter)][field]: str = 'unknown'
                    else:
                        try:
                            result[str(counter)][field]: str = re.sub(
                                r'(\<(/?[^>]+)>)', '', self.soup[i].find_all(tag)[j].string
                            )
                        except:
    #                         print('Я три дня пытался это пофиксить')
                            pass

                counter += 1
            self.dict_with_fields: Dict[str, Dict[str, str]] = result

                
    def get_dict_with_fields(self) -> Dict[str, Dict[str, str]]:
        '''
        Возвращает форматированный словарь
        '''
        return self.dict_with_fields


    def save_to_json(self) -> None:
        '''
        Формирует джсон и сохраняет его в файл
        '''
        with open('output.json', 'w', encoding = 'utf-8') as output:
            json.dump(self.dict_with_fields, output, indent = 4)


# In[29]:


def main(ignore_actuality = False):
    '''
    LAB1
    '''
    P = Parser('URLs.csv', ignore_actuality)
    P.load_urls(); P.find_actual_urls(); P.download_data(); P.save_to_xml(); P.save_to_one_xml()
#     print(P.get_data())
    
    '''
    LAB2
    '''
    fields: List[str] = [
        'title',
        'source',
        'textBody',
        'pubDate',
        'url'
    ]
    tags: List[str] = [
        'title',
        'source',
        'description',
        'pubDate',
        'link'
    ]
    P.create_soup(); P.create_dict_with_fields(fields, tags); P.save_to_json()


# In[30]:


# get_ipython().run_cell_magic('time', '', 'main(ignore_actuality = True)')

if __name__ == '__main__':
    main(ignore_actuality = True)

