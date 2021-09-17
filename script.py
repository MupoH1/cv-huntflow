from __future__ import annotations

import argparse
import os
import glob
import sys
import unicodedata
import tempfile
import json
from typing import List

import requests
import xlrd
from requests import RequestException


class HuntFlowAPI:
    API_URL = 'https://dev-100-api.huntflow.dev'

    def __init__(self, token, account_id=None):
        self.api_token = token
        self.account_id = account_id or self.get_account_id()

    def request(self, url, method, headers=None, **kwargs):
        headers = headers or dict()
        headers.update({"Authorization": f"Bearer {self.api_token}"})
        url = f'{self.API_URL}{url}'
        if method == 'GET':
            responce = requests.get(url, headers=headers)
        elif method == 'POST':
            responce = requests.post(url, headers=headers, **kwargs)
        else:
            raise ValueError('Параметр method должен принимать значения GET/POST')
        return responce

    def me(self):
        responce = self.request(url='/me', method='GET')
        return responce

    def add_applicant(self, applicant_data):
        responce = self.request(url=f'/account/{self.account_id}/applicants', method='POST', json=applicant_data)
        return responce

    def add_applicant_to_vacancy(self, applicant_data, applicant_id):
        responce = self.request(url=f'/account/{self.account_id}/applicants/{applicant_id}/vacancy', method='POST', json=applicant_data)
        return responce

    def get_applicants(self):
        responce = self.request(url=f'/account/{self.account_id}/applicants', method='GET')
        return responce

    def get_accounts(self):
        responce = self.request(url='/accounts', method='GET')
        return responce

    def get_account_id(self):
        data = self.get_accounts().json()
        try:
            id = data['items'][0]['id']
            return id
        except LookupError as e:
            raise Exception(f'Не найдено приписанных к токену организаций\n request: /accounts;  responce: {data}')

    def get_vacancies(self):
        responce = self.request(url=f'/account/{self.account_id}/vacancies', method='GET')
        return responce

    def get_vacancy_statuses(self):
        responce = self.request(url=f'/account/{self.account_id}/vacancy/statuses', method='GET')
        return responce

    def get_vacancy_quotas(self, vacancy_id):
        responce = self.request(url=f'/account/{self.account_id}/vacancy/{vacancy_id}/quotas', method='GET')
        return responce

    def get_applicant_sources(self):
        responce = self.request(url=f'/account/{self.account_id}/applicant/sources', method='GET')
        return responce

    def post_file(self, file_path, name):
        if os.path.exists(file_path):
            content_types = {'.pdf': 'application/pdf', '.doc': 'application/msword', '.docx': 'application/msword'}
            file_ext = os.path.splitext(file_path)[-1]
            account_id = self.get_account_id()
            with open(file_path, 'rb') as file:
                files = {'file': (f'{name}{file_ext}', file, content_types.get(file_ext))}
                headers = {'X-File-Parse': 'true', }
                responce = self.request(url=f'/account/{account_id}/upload', method='POST', files=files,
                                        headers=headers)
            return responce


class Applicant:

    def __init__(self, position, first_name, last_name, status, salary_expectation=None, middle_name=None,
                 comment=None):
        self.huntflow_id = None
        self.position = position
        self.vacancy_id = None
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.salary_expectation = salary_expectation
        self.comment = comment
        self.status = status
        self.status_id = None
        self.cv_id = None
        self.cv_photo_id = None
        self.phones = []
        self.email = None
        self.skype = None
        self.telegram = None
        self.experience = []
        self.birth_day = None
        self.birth_month = None
        self.birth_year = None
        self.cv_text = None
        self.api = None

    def map_applicant_for_huntflow(self):
        data = {
            "last_name": self.last_name,
            "first_name": self.first_name,
            "middle_name": self.middle_name,
            "phone": self.phones[0] if len(self.phones) else None,
            "email": self.email,
            "position": self.position,
            "company": None,
            "money": self.salary_expectation,
            "birthday_day": self.birth_day,
            "birthday_month": self.birth_month,
            "birthday_year": self.birth_year,
            "photo": self.cv_photo_id,
            "externals": [
                {
                    "data": {
                        "body": self.cv_text
                    },
                    "auth_type": "NATIVE",
                    "files": [
                        {
                            "id": self.cv_id
                        }
                    ],
                    "account_source": None
                }
            ]
        }
        return data

    def map_applicant_for_vacancy(self):
        data = {
            "vacancy": self.vacancy_id,
            "status": self.status_id,
            "comment": self.comment,
            "files": [
                {
                    "id": self.cv_id
                }
            ],
            "rejection_reason": None,
        }
        return data

    def get_fio(self):
        return ' '.join(filter(lambda x: x is not None, [self.last_name, self.first_name, self.middle_name]))

    def update_from_huntflow_cv(self, api):
        wildcard_path = unicodedata.normalize('NFKD', f'{os.path.join(self.position, self.get_fio())}*')
        glob_variants = glob.glob(wildcard_path)
        file_path = glob_variants[0] if len(glob_variants) else ''
        data = api.post_file(file_path, self.get_fio())
        if not data:
            return
        data = data.json()
        self.cv_id = data.get('id')
        self.cv_text = data.get('text')
        if data.get('photo'):
            self.cv_photo_id = data.get('photo').get('id')
        if data.get('fields'):
            fields = data.get('fields')
            self.phones = fields.get('phones', list())
            self.email = fields.get('email')
            self.skype = fields.get('skype')
            self.telegram = fields.get('telegram')
            self.experience = fields.get('experience', list())
            if fields.get('birthdate'):
                birthdate = fields.get('birthdate')
                self.birth_day = birthdate.get('day')
                self.birth_month = birthdate.get('month')
                self.birth_year = birthdate.get('year')

    @staticmethod
    def set_vacancy_ids(applicant_list: List[Applicant], api):
        data = api.get_vacancies().json()
        if data.get('items'):
            ids = [item['id'] for item in data.get('items') if item.get('id')]
            positions = [item['position'] for item in data.get('items') if item.get('position')]
            vacancies_dict = dict(zip(positions, ids))
            for applicant in applicant_list:
                applicant.vacancy_id = vacancies_dict.get(applicant.position)

    @staticmethod
    def set_status_ids(applicant_list: List[Applicant], api):
        data = api.get_vacancy_statuses().json()
        if data.get('items'):
            ids = [item['id'] for item in data.get('items') if item.get('id')]
            names = [item['name'] for item in data.get('items') if item.get('name')]
            statuses_dict = dict(zip(names, ids))
            for applicant in applicant_list:
                applicant.status_id = statuses_dict.get(applicant.status)

    @classmethod
    def __from_dict(cls, data: dict) -> Applicant:
        name_variants = ['last_name', 'first_name', 'middle_name']
        splitted = [name.strip() for name in data.pop('fio').strip().split(' ')]
        names = dict(zip(name_variants, splitted))
        data.update(names)
        return cls(**data)

    @classmethod
    def create_from_xls(cls, file_path) -> List[Applicant]:
        row_titles = ['position', 'fio', 'salary_expectation', 'comment', 'status']
        workbook = xlrd.open_workbook(file_path)
        worksheet = workbook.sheet_by_index(0)
        candidates = []
        for row_index in range(1, worksheet.nrows):
            row = []
            for col_index in range(worksheet.ncols):
                row.append(worksheet.cell(row_index, col_index).value)
            row_dict = dict(zip(row_titles, row))
            candidates.append(Applicant.__from_dict(row_dict))
        return candidates


def process_data(token, data_path):
    api = HuntFlowAPI(token)
    temp_file = tempfile.TemporaryFile()
    temp_file.name = f'{data_path}_position.data'
    applicants = Applicant.create_from_xls(data_path)
    Applicant.set_status_ids(applicants, api)
    Applicant.set_vacancy_ids(applicants, api)
    for position, applicant in enumerate(applicants):
        start_from = None
        if os.path.exists(temp_file.name):
            start_from = int(open(temp_file.name, 'r').read())
        if start_from:
            if position < start_from:
                continue
        try:
            applicant.update_from_huntflow_cv(api)
            response = api.add_applicant(applicant.map_applicant_for_huntflow()).json()
            print(response)
            applicant.huntflow_id = response.get('id')
            response = api.add_applicant_to_vacancy(applicant.map_applicant_for_vacancy(), applicant.huntflow_id).json()
            print(response)
        except RequestException as err:
            if position:
                temp_file = open(temp_file.name, 'w')
                temp_file.write(str(position))
            temp_file.close()
            raise
    if os.path.exists(temp_file.name):
        os.remove(temp_file.name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help='Путь к файлу в формате xls/xlsx')
    parser.add_argument("--tkn", help='Токен для API Huntflow')
    args = parser.parse_args()
    if not args.path or not args.tkn:
        raise ValueError("Обязательные аргументы для ввода --path, --tkn")
    data_path = args.path
    token = args.tkn
    process_data(token, data_path)
