import requests
from config import config
from utils import create_database, save_in_table


def main():
    company_response = requests.get("https://jsonplaceholder.typicode.com/posts/").json()
    params = config()
    create_database('gazprom', params)
    save_in_table(company_response, 'gazprom', params)


if __name__ == "__main__":
    main()
