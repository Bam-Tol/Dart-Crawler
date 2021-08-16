# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 14:28:36 2021

@author: GeonHo
"""

import requests
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
import time
import copy
from html_table_parser import parser_functions as parser
from io import BytesIO
from zipfile import ZipFile
from xml.etree.ElementTree import parse


## 보고서명에 전환사채 또는 전환청구권 단어가 포함된 보고서만 추출
def collect_report(url, params, page_count):
    df = pd.DataFrame()
    # pattern = re.compile('전환사채|전환청구권')
    ## 'B': 주요사항 보고, 'I': 거래소공시
    for pblntf_ty in ['B', 'I']:
        params['pblntf_ty'] = pblntf_ty
        response = requests.get(url, params=params)
        total_pages = response.json()['total_page']
        ## 주요사항 보고서를 df에 추가
        for page_no in range(1, total_pages+1):
            start = time.time()    
            print(f'호출 {page_no} / {total_pages} 페이지')
            params['page_no'] = page_no
            response = requests.get(url, params=params)
            
            ## 마지막 페이지는 page_count 보다 작음
            if page_no == total_pages:
                elem_range = len(response.json()['list'])
            else:
                elem_range = page_count
    
            ## 페이지 내의 모든 보고서 가져오기
            for elem in range(elem_range):
                component = response.json()['list'][elem]
                df = df.append([component])
                
                # ## '전환사채' 또는 '전환청구권'이 보고서명에 들어가면 df에 추가
                # if pattern.search(component['report_nm']) is not None:
                #     df = df.append([component])
            end = time.time()
            running_time = end - start
            print(round(running_time, 4), '초')
            if running_time < 0.8:
                time.sleep(0.8 - running_time)              

    if df.shape[0] > 0:
        ## 2012 교환사채
        df['Tag'] = df['report_nm'].apply(lambda x: '전환청구권행사' if re.search('전환청구권행사', x) is not None else x)
        df['Tag'] = df['Tag'].apply(lambda x: '전환사채권발행결정' if re.search('전환사채권발행결정|전환사채발행결정|전환사채발행결의|교환사채권발행결정', x) is not None else x)
    else:
        df = pd.DataFrame(columns = ['corp_code', 'corp_name', 'stock_code', 
                                     'corp_cls', 'report_nm', 'rcept_no', 
                                     'flr_nm', 'rcept_dt', 'rm', 'Tag'])
    return df


## 전환사채권발행결정 보고서 크롤링
def collect_publish_rights(df, publish_rights_URL, driver):
    publish_rights = copy.deepcopy(df[df['Tag'] == '전환사채권발행결정'].reset_index(drop=True))
    counts = publish_rights.shape[0]
    print('전환사채권발행결정 보고서 수:', counts)

    ## 크롤링할 패턴 정의
    ## 회차, 권면총액, 사채만기일, 전환청구기간 시작일, 전환청구기간 종료일, 청약일, 납입일, 이사회결정일
    count_pattern = re.compile("회차 (\d+)") ## 회차 패턴
    doc_type_pattern = re.compile("\d+ 종류 ([\w\s]+) \d\.") ## 종류 패턴
    total_amount_pattern = re.compile("\d+\. 사채의 권면\(전자등록\)총액 \(원\) ([\d+\,]+)") ## 권면총액 패턴
    total_amount_pattern_ = re.compile("\d+\. 사채의 권면총액 \(원\) ([\d+\,]+)") ## 권면총액 패턴2
    total_amount_pattern__ = re.compile("원화기준[\s]*\(원\) ([\d+\,]+) ") ## 해외전환사채 권면총액 패턴
    due_date_pattern = re.compile("\d+\. 사채만기일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+)") ## 사채만기일 패턴
    due_date_pattern_ = re.compile("\d+\. 만기일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+)") 
    due_date_pattern__ = re.compile("\d+\. 사채만기 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+)") 
    claim_start_date_pattern = re.compile("전환청구기간 시작일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 전환청구기간 시작일 패턴
    claim_start_date_pattern_ = re.compile("전환가능기간 시작일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 해외 사채 전환가기간 시작일 패턴
    claim_end_date_pattern = re.compile("전환청구기간 [\d|\w|\s|\.|\-]+ 종료일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 전환청구기간 종료일 패턴
    claim_end_date_pattern_ = re.compile("전환가능기간 [\d|\w|\s|\.|\-]+ 종료일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 해외 사채 전환가기간 종료일 패턴
    subscription_date_pattern = re.compile("\d+\. 청약일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 청약일 패턴
    payment_date_pattern = re.compile("\d+\. 납입일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 납입일 패턴
    payment_date_pattern_ = re.compile("\d+\. 발행예정일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 해외전환사채 납입일 패턴
    board_decision_date_pattern = re.compile("\d+\. 이사회결의일\(결정일\) (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") ## 이사회결정일 패턴
    board_decision_date_pattern_ = re.compile("\d+\. 이사회결의일 (\d+년[\s]*\d+월[\s]*\d+일|\d+\.[\s]*\d+\.[\s]*\d+|\d+\-\d+\-\d+|\-)") 

    ## 금융위원회 귀중 이하 ~ 기타 투자판단에 찹고할 사항까지만 탐색
    substring_start_pattern = re.compile("금융위원회[\s]*\/[\s]*한국거래소[\s]*귀중|금융위원회[\s]*귀중|금융위원회\(금융감독원\)[\s]*귀중|금융감독위원회[\s]*\(금융감독원\)[\s]*귀중")
    substring_end_pattern = re.compile("기타 투자판단에 참고할 사항|기타투자판단에 참고할 사항")    
    
    result_columns = ['rcept_no', 'corp_code', 'corp_name', '회차', '종류', '권면총액', '이사회결정일', 
                      '청약일', '납입일(발행예정일)', '전환청구기간시작일', '전환청구기간종료일', '사채만기일', 
                      '최종문서여부', '최종문서여부판단', '정상수집여부', 'url', 'error_log']
    result_publish_rights = pd.DataFrame(columns=result_columns)
    counts=100
    for elem in range(counts):
        start = time.time()
        print(f'{elem+1} / {counts}')
        try:
            rcept_no = publish_rights['rcept_no'][elem] ## 보고서 번호            
            corp_code = publish_rights['corp_code'][elem] ## 기업 번호
            corp_name = publish_rights['corp_name'][elem] ## 기업 이름
            print(rcept_no, corp_code, corp_name)
            
            ## 호출
            publish_rights_url = publish_rights_URL + '?' + 'rcpNo=' + str(rcept_no)
            # publish_rights_url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20200819000203'
            driver.get(publish_rights_url)
            print('호출', publish_rights_url)
            
            ## 최종문서 여부
            bs_ = BeautifulSoup(driver.page_source, "lxml")
            center = bs_.find('div', {'id':'center'}).get_text().strip()
            if center == '':
                final_status = '최종문서'
                print('최종문서')
            else:
                final_status = '최종문서가 아님'
                print(center)
            
            ## iframe 가져오기(iframe은 Xpath로 가져올 수 없음)
            ## 페이지 이동에 따른 딜레이
            driver.switch_to.frame('ifrm')
            about_blank_check = driver.current_url
            while about_blank_check == 'about:blank':
                time.sleep(0.01)
                print('0.01초 대기')
                about_blank_check = driver.current_url
                
            ## 주요사항보고서만 가져오기
            try:
                start_index = substring_start_pattern.search(driver.page_source).span()[0] ## 시작부분
                end_index = len(driver.page_source)
                for end_index in substring_end_pattern.finditer(driver.page_source):
                    end_index = end_index.span()[1] ## 끝부분    
                    
                main_report_string = driver.page_source[start_index:end_index]
                
                ## BeautifulSoup 객체로 변환
                bs = BeautifulSoup(main_report_string, "lxml")
                substring = re.sub("\\s+", " ", bs.get_text())
                        
            except:
                start_index = 0
                end_index = len(driver.page_source)
                for end_index in substring_end_pattern.finditer(driver.page_source):
                    end_index = end_index.span()[1] ## 끝부분
                    
                main_report_string = driver.page_source[start_index:end_index]
                
                ## BeautifulSoup 객체로 변환
                bs = BeautifulSoup(main_report_string, "lxml")
                substring = re.sub("\\s+", " ", bs.get_text())
                pattern = re.compile('(?=1\.[\s]*사채의[\s]*종류[\s]*회차[\s\d]*종류).*')
                substring = pattern.findall(substring)[0]
                
            ## 원하는 정보만 빼내기
            count = count_pattern.findall(substring) ## 회차 추출      
            doc_type = doc_type_pattern.findall(substring)
            total_amount = total_amount_pattern.findall(substring) ## 권면총액 추출
            if len(total_amount) == 0:
                total_amount = total_amount_pattern_.findall(substring)
                if len(total_amount) == 0:
                    total_amount = total_amount_pattern__.findall(substring)
            board_decision_date = board_decision_date_pattern.findall(substring) ## 이사회결정일 추출
            if len(board_decision_date) == 0:
                board_decision_date = board_decision_date_pattern_.findall(substring)
            subscription_date = subscription_date_pattern.findall(substring) ## 청약일 추출
            if len(subscription_date) == 0:
                subscription_date = ['-']
            payment_date = payment_date_pattern.findall(substring) ## 납입일 추출
            if len(payment_date) == 0:
                payment_date = payment_date_pattern_.findall(substring)
            claim_start_date = claim_start_date_pattern.findall(substring) ## 전환청구기간 시작일 추출
            if len(claim_start_date) == 0:
                claim_start_date = claim_start_date_pattern_.findall(substring)
            claim_end_date = claim_end_date_pattern.findall(substring) ## 전환청구기간 종료일 추출
            if len(claim_end_date) == 0:
                claim_end_date = claim_end_date_pattern_.findall(substring) 
            due_date = due_date_pattern.findall(substring) ## 사채만기일 추출
            if len(due_date) == 0:
                due_date = due_date_pattern_.findall(substring)
                if len(due_date) == 0:
                    due_date = due_date_pattern__.findall(substring)
                

            ## DataFrame으로 변환
            # rcept_no, corp_code, corp_name = 1, 1, 1
            tmp_columns = ['rcept_no', 'corp_code', 'corp_name', '회차', '종류', '권면총액', '이사회결정일', 
                           '청약일', '납입일(발행예정일)', '전환청구기간시작일', '전환청구기간종료일', '사채만기일', '최종문서여부', '최종문서여부판단']
            tmp_values = [rcept_no, corp_code, corp_name, count, doc_type, total_amount, board_decision_date, 
                          subscription_date, payment_date, claim_start_date, claim_end_date, due_date, final_status, center]
            tmp = pd.DataFrame(dict(zip(tmp_columns, tmp_values)))
            tmp['정상수집여부'] = '정상'
            print('정상 수집')
            

        except Exception as ex:
            tmp_columns = ['rcept_no', 'corp_code', 'corp_name']
            tmp = pd.DataFrame([dict(zip(tmp_columns, [rcept_no, corp_code, corp_name]))])
            tmp['정상수집여부'] = '비정상'
            print('비정상 수집, 로그를 확인하세요')
            print(f"'type': {type(ex).__name__}, 'message': {str(ex)}")
            
            trace = []
            tb = ex.__traceback__
            while tb is not None:
                trace.append({
                              "filename": tb.tb_frame.f_code.co_filename,
                              "name": tb.tb_frame.f_code.co_name,
                              "lineno": tb.tb_lineno
                              })
                tb = tb.tb_next
            
            tmp['error_log'] = [{'type': type(ex).__name__, 'message': str(ex), 'trace': trace}]
        
        ## 결과 테이블에 append
        tmp['url'] = publish_rights_url
        result_publish_rights = pd.concat([result_publish_rights, tmp])
        
        end = time.time()
        running_time = end - start
        # print(round(running_time, 4), '초 \n')
        
        ## 호출 한 번 per 0.8초로 제한
        if running_time < 0.8:
            time.sleep(0.8 - running_time)   
            
    result_publish_rights = result_publish_rights.reset_index(drop=True)
    return result_publish_rights


## 전환청구권행사 크롤링
def collect_exercise_rights(df, exci_rights_URL, driver):

    exci_rights = copy.deepcopy(df[df['Tag'] == '전환청구권행사'].reset_index(drop=True))
    counts = exci_rights.shape[0]
    print('전환청구행사 보고서 수:', counts)

    result_columns = ['rcept_no', 'corp_code', 'corp_name', '회차', '청구일자', '종류', 
                      '청구금액', '전환가액', '발행한 주식수', '상장일 또는 예정일', 
                      '최종문서여부', '최종문서여부판단', '정상수집여부', 'url', 'error_log']

    result_exci_rights = pd.DataFrame(columns=result_columns)
    for elem in range(counts):
        start = time.time()
        print(f'{elem+1} / {counts}')
        try:
            ## 보고서 번호
            rcept_no = exci_rights['rcept_no'][elem]
            ## 기업 번호
            corp_code = exci_rights['corp_code'][elem]
            ## 기업 이름
            corp_name = exci_rights['corp_name'][elem]
            print(rcept_no, corp_code, corp_name)
            
            ## 호출
            exci_rights_url = exci_rights_URL + '?' + 'rcpNo=' + str(rcept_no)
            # exci_rights_url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20130528900001'
            driver.get(exci_rights_url)
            print('호출', exci_rights_url)
            
            ## 최종문서 여부
            bs_ = BeautifulSoup(driver.page_source, "lxml")
            center = bs_.find('div', {'id':'center'}).get_text().strip()
            if center == '':
                final_status = '최종문서'
                print('최종문서')
            else:
                final_status = '최종문서가 아님'
                print(center)
                       
            ## iframe 가져오기(iframe은 Xpath로 가져올 수 없음)
            ## 페이지 이동에 따른 딜레이
            driver.switch_to.frame('ifrm')
            about_blank_check = driver.current_url
            while about_blank_check == 'about:blank':
                time.sleep(0.01)
                print('0.01초 대기')
                about_blank_check = driver.current_url
            
            ## html로 가져오기
            bs = BeautifulSoup(driver.page_source, "lxml")
            try:
                ## 테이블 id로 원하는 테이블 가져오기
                tags = bs.find('table', {'id': 'XFormD27_Form0_RepeatTable0'})
                ## table parsing
                html_table = parser.make2d(tags)
            
            except:
                ## 청구일자, 회차가 존재하는 테이블 가져와서 파싱하기
                pattern = re.compile('(?=청구일자).*(?=회차).*')
                tags = [x for x in bs.find_all('table') if pattern.findall(re.sub("\\s+", " ", x.get_text()))][0]
                html_table = parser.make2d(tags)
                
            ## dataframe으로 변환
            tmp_columns = ['청구일자', '회차', '종류', '청구금액', '전환가액', '발행한 주식수', '상장일 또는 예정일']
            try:
                tmp = pd.DataFrame(html_table[2:], columns=tmp_columns)
            except:
                tmp = pd.DataFrame(html_table[2:], columns=html_table[1])
                tmp.pop('청구권자')
                tmp.columns = tmp_columns
                
            print("청구내역 수", len(tmp))
            
            tmp['rcept_no'] = [rcept_no] * len(tmp)
            tmp['corp_code'] = [corp_code] * len(tmp)
            tmp['corp_name'] = [corp_name] * len(tmp)        
            tmp['최종문서여부'] = [final_status] * len(tmp)
            tmp['최종문서여부판단'] = [center] * len(tmp)
            tmp['정상수집여부'] = ['정상'] * len(tmp)
            print('정상 수집')
            
        except Exception as ex:
            tmp_columns = ['rcept_no', 'corp_code', 'corp_name']
            tmp = pd.DataFrame([dict(zip(tmp_columns, [rcept_no, corp_code, corp_name]))])
            tmp['정상수집여부'] = '비정상'
            print('비정상 수집, 로그를 확인하세요')
            print(f"'type': {type(ex).__name__}, 'message': {str(ex)}")
            
            trace = []
            tb = ex.__traceback__
            while tb is not None:
                trace.append({
                              "filename": tb.tb_frame.f_code.co_filename,
                              "name": tb.tb_frame.f_code.co_name,
                              "lineno": tb.tb_lineno
                              })
                tb = tb.tb_next
            
            tmp['error_log'] = [{'type': type(ex).__name__, 'message': str(ex), 'trace': trace}]
        
        ## 결과 테이블에 append
        tmp['url'] = exci_rights_url        
        result_exci_rights = pd.concat([result_exci_rights, tmp])

        end = time.time()
        running_time = end - start
        # print(round(running_time, 4), '초 \n')
        
        ## 호출 한 번 per 0.8초로 제한
        if running_time < 0.8:
            time.sleep(0.8 - running_time)   
            
    result_exci_rights = result_exci_rights.reset_index(drop=True)
    return result_exci_rights


class BreakException(Exception):
    def __init__(self):
        ## Error 메시지
        super().__init__('일일 호출 한도 초과')

def check_limit(total_counts, limit, *args):
    total_counts += sum(args)
    if total_counts >= limit:
        print('일일 호출 한도 초과')
        raise BreakException()
    print(f'누적 호출 횟수 {total_counts}')
    return total_counts
