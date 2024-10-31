import psycopg2
from typing import List, Tuple


class DBManager:
    def __init__(self, dbname: str, user: str, password: str, host: str):
        db_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': '5433'
        }
        self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    def create_tables(self):
        commands = (
            """
            CREATE TABLE IF NOT EXISTS employers (
                employer_id INT PRIMARY KEY,
                name VARCHAR(255),
                description TEXT,
                site_url VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS vacancies (
                vacancy_id INT PRIMARY KEY,
                employer_id INT,
                name VARCHAR(255),
                description TEXT,
                salary_from INT,
                salary_to INT,
                url VARCHAR(255),
                avg_salary FLOAT,
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            )
            """
        )
        for command in commands:
            self.cur.execute(command)
        self.conn.commit()

    def insert_employers(self, employers: List[dict]):
        for employer in employers:
            self.cur.execute(
                """
                INSERT INTO employers (employer_id, name, description, site_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (employer_id) DO NOTHING
                """,
                (employer['id'], employer['name'], employer['description'], employer['site_url'])
            )
        self.conn.commit()

    def insert_vacancies(self, vacancies: List[dict]):
        for vacancy in vacancies:
            salary_from = vacancy['salary']['from'] if vacancy['salary'] else None
            salary_to = vacancy['salary']['to'] if vacancy['salary'] else None
            description = vacancy.get('description', '')  # Используйте метод get для обработки отсутствующих ключей
            self.cur.execute(
                """
                INSERT INTO vacancies (vacancy_id, employer_id, name, description, salary_from, salary_to, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vacancy_id) DO NOTHING
                """,
                (vacancy['id'], vacancy['employer']['id'], vacancy['name'], description,
                 salary_from, salary_to, vacancy['alternate_url'])
            )
        self.conn.commit()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        self.cur.execute(
            """
            SELECT e.name, COUNT(v.vacancy_id)
            FROM employers e
            LEFT JOIN vacancies v ON e.employer_id = v.employer_id
            GROUP BY e.name
            """
        )
        return [(name, count) for name, count in self.cur.fetchall()]

    def get_all_vacancies(self) -> List[Tuple[str, str, int, int, str]]:
        self.cur.execute(
            """
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM employers e
            JOIN vacancies v ON e.employer_id = v.employer_id
            """
        )
        return [(employer_name, vacancy_name, salary_from, salary_to, url) for
                employer_name, vacancy_name, salary_from, salary_to, url in self.cur.fetchall()]

    def get_avg_salary(self) -> float:
        self.cur.execute(
            """
            SELECT AVG((salary_from + salary_to) / 2)
            FROM vacancies
            WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
            """
        )
        avg_salary = self.cur.fetchone()[0]

        # Создаем новую таблицу для хранения средней зарплаты
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS average_salaries (
                avg_salary FLOAT
            )
            """
        )
        self.conn.commit()

        # Вставляем среднюю зарплату в таблицу average_salaries
        self.cur.execute(
            """
            INSERT INTO average_salaries (avg_salary)
            VALUES (%s)
            """,
            (avg_salary,)
        )
        self.conn.commit()

        return avg_salary

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, str, int, int, str]]:
        avg_salary = self.get_avg_salary()
        self.cur.execute(
            """
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM employers e
            JOIN vacancies v ON e.employer_id = v.employer_id
            WHERE (v.salary_from + v.salary_to) / 2 > %s
            """,
            (avg_salary,)
        )
        return [(employer_name, vacancy_name, salary_from, salary_to, url) for
                employer_name, vacancy_name, salary_from, salary_to, url in self.cur.fetchall()]

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, str, int, int, str]]:
        self.cur.execute(
            """
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM employers e
            JOIN vacancies v ON e.employer_id = v.employer_id
            WHERE v.name LIKE %s
            """,
            (f'%{keyword}%',)
        )
        return [(employer_name, vacancy_name, salary_from, salary_to, url) for
                employer_name, vacancy_name, salary_from, salary_to, url in self.cur.fetchall()]

    def close(self):
        self.cur.close()
        self.conn.close()
