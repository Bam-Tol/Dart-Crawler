# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 14:28:36 2021
@author: GeonHo
"""

import pandas as pd
from selenium import webdriver
import datetime
from datetime import timedelta
from datetime import datetime as dt
import math
import crawling_dart_module as DM


## 보고서 추출 url
## 고정값
corp_code_URL = 'https://opendart.fss.or.kr/api/corpCode.xml'
jurir_URL = 'https://opendart.fss.or.kr/api/company.json'
base_URL = 'https://opendart.fss.or.kr/api/list.json' ## 보고서리스트 url
publish_rights_URL = 'http://dart.fss.or.kr/dsaf001/main.do' ## 전환사채권발행결정 보고서 url
exci_rights_URL = 'http://dart.fss.or.kr/dsaf001/main.do' ## 전환청구권행사 보고서 url

crtfc_key = 'ce6cd1b35f397ec5743bc5671e26f2950441bc16' ## 내꺼

## 드라이버 정의
driver = webdriver.PhantomJS('C:/chromedriver/phantomjs-2.1.1-windows/bin/phantomjs.exe')


## 호출 한도
limit_calling_counts = 0
limit = 9000

year_list = [2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
for year in year_list:
    ## 초깃값
    if year > 2011:
        crtfc_key = '26d192e1a195c89e7e28d3ce8a4c2c0cc1442fb4'

    start = datetime.date(year, 1, 1)
    end = datetime.date(year, 12, 31)
    bgn_dt = start
    end_dt = bgn_dt + timedelta(days=90)
    bgn_de = int(dt.strftime(bgn_dt, "%Y%m%d"))
    end_de = int(dt.strftime(end_dt, "%Y%m%d"))
    page_count = 100 # 페이지 당 건 수(최대값:100)    
    page_no = 1
    ## 주요사항보고
    payload = {'crtfc_key': crtfc_key, 'bgn_de': bgn_de, 'end_de': end_de, 'page_count': page_count,
               'page_no': page_no, 'last_reprt_at': 'Y', 'pblntf_ty': 'NaN'}
    ## 90일 단위로
    max_iter = math.ceil((end - start).days / 90)
    
    ## 결과 df
    result_extracted_report = None
    result_publish_rights = None
    result_excerise_rights = None
    
    iteration = 1
    while iteration <= max_iter:
        print(f'{iteration}/{max_iter}')
        try:
            print(f'{bgn_dt}부터 {end_dt}까지')
        
            ## 1. 보고서명에 전환사채 또는 전환청구권 단어가 포함된 보고서만 추출
            ## PK = rcept_no
            extracted_report = DM.collect_report(base_URL, payload, page_count) # 52, 127
            
            ## 2. 전환사채권발행결정 보고서 크롤링
            ## PK = rcept_no
            extracted_report = pd.DataFrame(columns=['rcept_no', 'corp_code', 'corp_name'])
            publish_rights = DM.collect_publish_rights(extracted_report, publish_rights_URL, driver)

            ## 3. 전환청구권행사 보고서 크롤링
            ## PK = rcept_no, 회차, 청구금액, 상장일 또는 예정일(이렇게 해도 중복 가능성 있음)
            excerise_rights = DM.collect_exercise_rights(extracted_report, exci_rights_URL, driver)
            
            result_extracted_report = pd.concat([result_extracted_report, extracted_report])
            result_publish_rights = pd.concat([result_publish_rights, publish_rights])
            result_excerise_rights = pd.concat([result_excerise_rights, excerise_rights])
    
        except:
            result_extracted_report.to_csv(f'보고서 리스트_{year}.csv', encoding='cp949')
            result_publish_rights.to_csv(f'전환사채권발행결정 보고서_{year}.csv', encoding='cp949')
            try:
                result_excerise_rights.to_csv(f'전환청구권행사 보고서_{year}.csv', encoding='cp949') 
            except:
                result_excerise_rights.to_csv(f'전환청구권행사 보고서_{year}.csv', encoding='utf-8')
            break
        
        ## 90일씩 이동
        bgn_dt += timedelta(days=91)
        end_dt += timedelta(days=91)
        if end_dt > end:
            end_dt = end
            iteration = max_iter - 1
        bgn_de = int(dt.strftime(bgn_dt, "%Y%m%d"))
        end_de = int(dt.strftime(end_dt, "%Y%m%d"))
        payload['bgn_de'] = bgn_de
        payload['end_de'] = end_de
        
        iteration += 1
        
    result_extracted_report.to_csv(f'보고서 리스트_{year}.csv', encoding='cp949')
    result_publish_rights.to_csv(f'전환사채권발행결정 보고서_{year}.csv', encoding='cp949')
    try:
        result_excerise_rights.to_csv(f'전환청구권행사 보고서_{year}.csv', encoding='cp949') 
    except:
        result_excerise_rights.to_csv(f'전환청구권행사 보고서_{year}.csv', encoding='utf-8')

