import requests


def get_employers(employer_ids):
    employers = []
    for employer_id in employer_ids:
        url = f'https://api.hh.ru/employers/{employer_id}'
        response = requests.get(url)
        if response.status_code == 200:
            employers.append(response.json())
    return employers


def get_vacancies(employer_id):
    url = f'https://api.hh.ru/vacancies?employer_id={employer_id}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['items']
    return []
