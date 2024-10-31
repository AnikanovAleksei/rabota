import os
from dotenv import load_dotenv
from api_interaction import get_employers, get_vacancies
from db_interaction import DBManager


def main():
    load_dotenv()
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')

    db_manager = DBManager(dbname, user, password, host)
    db_manager.create_tables()

    employer_ids = [1740, 3529, 15478, 2180, 39305, 80, 1057, 3776, 1122462, 78638]
    employers = get_employers(employer_ids)
    db_manager.insert_employers(employers)

    for employer in employers:
        vacancies = get_vacancies(employer['id'])
        db_manager.insert_vacancies(vacancies)

    print("Companies and vacancies count:")
    for company, count in db_manager.get_companies_and_vacancies_count():
        print(f"{company}: {count} vacancies")

    print("\nAll vacancies:")
    for vacancy in db_manager.get_all_vacancies():
        print(vacancy)

    avg_salary = db_manager.get_avg_salary()
    print(f"\nAverage salary: {avg_salary}")

    print("\nVacancies with higher salary:")
    for vacancy in db_manager.get_vacancies_with_higher_salary():
        print(vacancy)

    keyword = "python"
    print(f"\nVacancies with keyword '{keyword}':")
    for vacancy in db_manager.get_vacancies_with_keyword(keyword):
        print(vacancy)

    db_manager.close()


if __name__ == "__main__":
    main()
